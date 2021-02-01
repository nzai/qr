package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/stores"
	"github.com/nzai/qr/utils"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	tdeDriverName = "taosSql"
)

type companyQuote struct {
	Exchange    string
	CompanyCode string
	Date        time.Time
	quotes.SerialType
	quotes.Quote
}

type rollup struct {
	sourceStore         stores.Store
	db                  *sql.DB
	exchanges           []exchanges.Exchange
	in                  chan *quotes.ExchangeDailyQuote
	out                 chan *companyQuote
	retryTimes          int
	writePool           int
	companyTableCreated map[string]bool
}

func (s *rollup) Command() *cli.Command {
	return &cli.Command{
		Name:  "rollup",
		Usage: "rollup 1m to 1d",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "source",
				Aliases:  []string{"s"},
				Required: true,
				Usage:    "\033[1;33mRequired!\033[0m specify source",
			},
			&cli.StringFlag{
				Name:     "dest",
				Aliases:  []string{"d"},
				Required: false,
				Usage:    "specify dest",
				Value:    "tcp(127.0.0.1:6030)/quotes?interpolateParams=true",
			},
			&cli.StringFlag{
				Name:     "exchanges",
				Aliases:  []string{"e"},
				Required: false,
				Usage:    "specify exchanges",
				Value:    "Nasdaq,Amex,Nyse,Sse,Szse,Hkex",
			},
			&cli.IntFlag{
				Name:     "retry_times",
				Aliases:  []string{"retry"},
				Required: false,
				Usage:    "specify retry times(default 5)",
				Value:    5,
			},
			&cli.IntFlag{
				Name:     "write_pool",
				Aliases:  []string{"wp"},
				Required: false,
				Usage:    "specify write pool size(default 128)",
				Value:    128,
			},
		},
		Action: func(c *cli.Context) error {
			defer s.close()

			source := c.String("source")
			dest := c.String("dest")
			_exchangeNames := c.String("exchanges")
			retryTimes := c.Int("retry_times")
			writePool := c.Int("write_pool")

			zap.L().Debug("rollup", zap.String("source", source),
				zap.String("dest", dest),
				zap.String("exchanges", _exchangeNames),
				zap.Int("retryTimes", retryTimes),
				zap.Int("writePool", writePool))

			s.retryTimes = retryTimes
			s.writePool = writePool
			s.companyTableCreated = make(map[string]bool)

			sourceStore, err := stores.Parse(source)
			if err != nil {
				zap.L().Error("parse source store argument failed",
					zap.Error(err),
					zap.String("source", source))
				return err
			}
			s.sourceStore = sourceStore

			err = s.initTDEngine(dest)
			if err != nil {
				return err
			}

			_exchanges, err := exchanges.Parse(_exchangeNames)
			if err != nil {
				zap.L().Error("parse exchange argument failed",
					zap.Error(err),
					zap.String("exchanges", _exchangeNames))
				return err
			}
			s.exchanges = _exchanges

			err = s.ensureTables()
			if err != nil {
				return err
			}

			s.in = make(chan *quotes.ExchangeDailyQuote, 8)
			s.out = make(chan *companyQuote, s.writePool*4)

			go s.readLoop()
			go s.writeLoop()

			return s.process()
		},
	}
}

func (s rollup) close() {
	if s.sourceStore != nil {
		s.sourceStore.Close()
	}

	if s.db != nil {
		s.db.Close()
	}

	// if s.in != nil {
	// 	close(s.in)
	// }

	// if s.out != nil {
	// 	close(s.out)
	// }
}

func (s *rollup) initTDEngine(address string) error {
	db, err := sql.Open(tdeDriverName, address)
	if err != nil {
		zap.L().Error("connect tdengine failed", zap.Error(err), zap.String("address", address))
		return err
	}

	err = db.Ping()
	if err != nil {
		zap.L().Error("ping tdengine failed", zap.Error(err), zap.String("address", address))
		return err
	}

	s.db = db

	return nil
}

func (s rollup) ensureTables() error {
	commands := []string{
		"CREATE TABLE IF NOT EXISTS quotes (ts timestamp, open float, close float, high float, low float, volume bigint) TAGS (exchange binary(32), company binary(32), type binary(16))",
		"CREATE TABLE IF NOT EXISTS symbols (ts timestamp, symbol binary(32), name nchar(256)) TAGS (exchange binary(32), type binary(64))",
		"CREATE TABLE IF NOT EXISTS flags (ts timestamp, flag bigint) TAGS (exchange binary(32), type binary(64))",
	}

	for _, exchange := range s.exchanges {
		commands = append(commands, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING flags TAGS ("%s", "raw_1d_done");`,
			s.exchangeDoneTableName(exchange.Code()),
			exchange.Code()))

		commands = append(commands, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING symbols TAGS ("%s", "company");`,
			s.exchangeCompaniesTableName(exchange.Code()),
			exchange.Code()))
	}

	for _, command := range commands {
		err := s.tryExecuteCommand(command)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s rollup) readLoop() {
	for _, exchange := range s.exchanges {
		date := utils.TodayZero(time.Now().In(exchange.Location()))
		startDate := time.Date(2015, 5, 1, 0, 0, 0, 0, exchange.Location())

		totalDays := int(date.Sub(startDate).Hours() / 24)

		zap.L().Info("start read source store",
			zap.String("exchange", exchange.Code()),
			zap.Time("start", date),
			zap.Time("end", startDate),
			zap.Int("totalDays", totalDays))

		for startDate.Before(date) {
			start := time.Now()
			for index := 1; index <= s.retryTimes; index++ {
				err := s.tryReadSourceOnce(exchange, date)
				if err == nil {
					break
				}

				if index == s.retryTimes {
					zap.L().Error("read source failed",
						zap.Error(err),
						zap.String("exchange", exchange.Code()),
						zap.Time("date", date),
						zap.Duration("duration", time.Since(start)))
				} else {
					time.Sleep(time.Second * 3)
				}
			}

			date = date.AddDate(0, 0, -1)
		}

		zap.L().Info("read exchange finished", zap.String("exchange", exchange.Code()))
	}

	close(s.in)
	zap.L().Info("read source finished")
}

func (s rollup) tryReadSourceOnce(exchange exchanges.Exchange, date time.Time) error {
	exists, err := s.destExists(exchange, date)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	exists, err = s.sourceStore.Exists(exchange, date)
	if err != nil {
		return err
	}

	if !exists {
		zap.L().Debug("source not exists",
			zap.String("exchange", exchange.Code()),
			zap.Time("start", date))
		return nil
	}

	zap.L().Info("read source",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	edq, err := s.sourceStore.Load(exchange, date)
	if err != nil {
		return err
	}

	zap.L().Info("read source success",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	s.in <- edq

	return nil
}

// destExists check quote exists
func (s rollup) destExists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	command := fmt.Sprintf("select flag from %s where ts=%d", s.exchangeDoneTableName(exchange.Code()), date.Unix()*1000)

	row := s.db.QueryRow(command)
	var flag int64
	err := row.Scan(&flag)
	if err == sql.ErrNoRows {
		return false, nil
	}

	if err != nil {
		if err.Error() == "Table does not exist" {
			return false, nil
		}

		zap.L().Error("query exists failed",
			zap.Error(err),
			zap.String("command", command),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	return flag > 0, nil
}

func (s rollup) process() error {
	for {
		edq, ok := <-s.in
		for companyCode, cdq := range edq.Quotes {
			s.out <- &companyQuote{
				Exchange:    edq.Exchange,
				CompanyCode: companyCode,
				Date:        edq.Date,
				SerialType:  quotes.SerialTypePre,
				Quote:       *cdq.Pre.Rollup(),
			}

			s.out <- &companyQuote{
				Exchange:    edq.Exchange,
				CompanyCode: companyCode,
				Date:        edq.Date,
				SerialType:  quotes.SerialTypeRegular,
				Quote:       *cdq.Regular.Rollup(),
			}

			s.out <- &companyQuote{
				Exchange:    edq.Exchange,
				CompanyCode: companyCode,
				Date:        edq.Date,
				SerialType:  quotes.SerialTypePost,
				Quote:       *cdq.Post.Rollup(),
			}
		}

		command := fmt.Sprintf("insert into %s values(%d, %d)", s.exchangeDoneTableName(edq.Exchange), edq.Date.Unix()*1000, 1)
		err := s.tryExecuteCommand(command)
		if err != nil {
			return err
		}

		zap.L().Info("exchange proccess success",
			zap.String("exchange", edq.Exchange),
			zap.Time("date", edq.Date))

		if ok {
			continue
		}

		break
	}

	close(s.out)
	return nil
}

func (s rollup) writeLoop() {
	for {
		cq, ok := <-s.out
		if cq.Timestamp == 0 {
			continue
		}

		err := s.ensureCompanyTable(cq)
		if err != nil {
			return
		}

		command := fmt.Sprintf("insert into %s values(%d, %f, %f, %f, %f, %d)",
			s.companySerialTableName(cq.Exchange, cq.CompanyCode, cq.SerialType),
			cq.Date.Unix()*1000, cq.Open, cq.Close, cq.High, cq.Low, cq.Volume)

		err = s.tryExecuteCommand(command)
		if err != nil {
			return
		}

		if !ok {
			return
		}
	}
}

func (s rollup) ensureCompanyTable(cq *companyQuote) error {
	key := fmt.Sprintf("%s:%s", cq.Exchange, cq.CompanyCode)
	exists := s.companyTableCreated[key]
	if exists {
		return nil
	}

	commands := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING quotes TAGS ("%s", "%s", "raw_1d_%s");`,
			s.companySerialTableName(cq.Exchange, cq.CompanyCode, quotes.SerialTypePre),
			cq.Exchange,
			cq.CompanyCode,
			quotes.SerialTypePre.String()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING quotes TAGS ("%s", "%s", "raw_1d_%s");`,
			s.companySerialTableName(cq.Exchange, cq.CompanyCode, quotes.SerialTypeRegular),
			cq.Exchange,
			cq.CompanyCode,
			quotes.SerialTypeRegular.String()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING quotes TAGS ("%s", "%s", "raw_1d_%s");`,
			s.companySerialTableName(cq.Exchange, cq.CompanyCode, quotes.SerialTypePost),
			cq.Exchange,
			cq.CompanyCode,
			quotes.SerialTypePost.String()),
	}

	for _, command := range commands {
		err := s.tryExecuteCommand(command)
		if err != nil {
			return err
		}
	}

	s.companyTableCreated[key] = true
	return nil
}

func (s rollup) exchangeDoneTableName(exchange string) string {
	return fmt.Sprintf("%s_raw_1d_done", strings.ToLower(exchange))
}

func (s rollup) exchangeCompaniesTableName(exchange string) string {
	return fmt.Sprintf("%s_company", strings.ToLower(exchange))
}

func (s rollup) companySerialTableName(exchange, companyCode string, serialType quotes.SerialType) string {
	return fmt.Sprintf("%s_%s_%s_raw_1d",
		strings.ToLower(exchange),
		strings.ToLower(companyCode),
		strings.ToLower(serialType.String()))
}

func (s rollup) tryExecuteCommand(command string) error {
	var err error
	for index := 1; index <= s.retryTimes; index++ {
		_, err = s.db.Exec(command)
		if err == nil {
			return nil
		}

		if index < s.retryTimes {
			time.Sleep(time.Second * 3)
			continue
		}
	}

	zap.L().Error("execute command failed",
		zap.Error(err),
		zap.String("command", command))

	return err
}
