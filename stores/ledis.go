package stores

import (
	"bytes"
	"fmt"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
	"go.uber.org/zap"
)

// exchange daily companies		type: hast	key: {exchange}:{date:yyyyMMdd}	field:{companyCode}					value:{companyCode},{companyName}
// company daily rollup quote	type: kv	key: {exchange}:{companyCode}:{date:yyyyMMdd} 						value:{timestamp},{open},{close},{high},{low},{volume}
// company daily quote serial	type: list	key: {exchange}:{companyCode}:{date:yyyyMMdd}:{pre|regular|post}	values:[{timestamp},{open},{close},{high},{low},{volume}]
// company daily dividend		type: hash	key: {exchange}:{companyCode}:dividend								value:{timestamp},{amount}
// company daily split			type: hash	key: {exchange}:{companyCode}:split									value:{timestamp},{numerator},{denominator}

const (
	datePattern     = "20060102"
	companyPattern  = "%s|%s"
	quotePattern    = "%d,%f,%f,%f,%f,%d"
	dividendPattern = "%d,%f"
	splitPattern    = "%d,%f,%f"
)

// Ledis define ledis store
type Ledis struct {
	instance *ledis.Ledis
	db       *ledis.DB
}

// NewLedis create new ledis store
func NewLedis(root string) *Ledis {
	conf := config.NewConfigDefault()
	conf.DataDir = root
	instance, err := ledis.Open(conf)
	if err != nil {
		zap.L().Fatal("init ledis store failed", zap.Error(err))
	}
	db, err := instance.Select(0)
	if err != nil {
		zap.L().Fatal("select ledis db failed", zap.Error(err))
	}

	return &Ledis{instance: instance, db: db}
}

// Close close ledis store
func (s Ledis) Close() error {
	if s.instance != nil {
		s.instance.Close()
	}

	return nil
}

func (s Ledis) exchangeKey(exchange exchanges.Exchange, date time.Time) []byte {
	return []byte(fmt.Sprintf("%s:%s", exchange.Code(), date.Format("20060102")))
}

func (s Ledis) companyKey(exchange exchanges.Exchange, company *quotes.Company, date time.Time) []byte {
	return []byte(fmt.Sprintf("%s:%s:%s", exchange.Code(), company.Code, date.Format("20060102")))
}

func (s Ledis) companySerialKey(exchange exchanges.Exchange, company *quotes.Company, date time.Time, serialType quotes.SerialType) []byte {
	return []byte(fmt.Sprintf("%s:%s:%s:%s", exchange.Code(), company.Code, date.Format("20060102"), serialType.String()))
}

func (s Ledis) dividendKey(exchange exchanges.Exchange, company *quotes.Company) []byte {
	return []byte(fmt.Sprintf("%s:%s:dividend", exchange.Code(), company.Code))
}

func (s Ledis) splitKey(exchange exchanges.Exchange, company *quotes.Company) []byte {
	return []byte(fmt.Sprintf("%s:%s:split", exchange.Code(), company.Code))
}

// Exists check quote exists
func (s Ledis) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	result, err := s.db.HKeyExists(s.exchangeKey(exchange, date))
	if err != nil {
		zap.L().Error("check exchange daily quote exists failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	return result > 0, nil
}

// Save save exchange daily quote
func (s Ledis) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	// save companies
	err := s.saveCompanies(exchange, date, edq.Companies)
	if err != nil {
		zap.L().Error("save exchange daily companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.Int("companies", len(edq.Companies)))
		return err
	}

	// save quotes
	err = s.saveCompanyQuotes(exchange, date, edq.Quotes)
	if err != nil {
		zap.L().Error("save exchange daily companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.Int("quotes", len(edq.Quotes)))
		return err
	}

	// load saved
	saved, err := s.Load(exchange, date)
	if err != nil {
		zap.L().Error("load saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	// validate
	err = edq.Equal(*saved)
	if err != nil {
		zap.L().Error("validate saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))

		// delete exchange daily key if validate failed
		err1 := s.remove(exchange, date, edq)
		if err1 != nil {
			zap.L().Error("remove saved exchange daily quote failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
		} else {
			zap.L().Info("remove saved exchange daily quote success",
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
		}

		return err
	}

	return nil
}

func (s Ledis) saveCompanies(exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) error {
	pairs := make([]ledis.FVPair, 0, len(companies))
	for companyCode, company := range companies {
		pairs = append(pairs, ledis.FVPair{
			Field: []byte(companyCode),
			Value: []byte(fmt.Sprintf(companyPattern, company.Code, company.Name)),
		})
	}

	err := s.db.HMset(s.exchangeKey(exchange, date), pairs...)
	if err != nil {
		zap.L().Error("save exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.Int("companies", len(companies)))
		return err
	}

	return nil
}

func (s Ledis) saveCompanyQuotes(exchange exchanges.Exchange, date time.Time, cdqs map[string]*quotes.CompanyDailyQuote) error {
	var err error
	for _, cdq := range cdqs {
		// save rollup quote
		rollup := cdq.Regular.Rollup()
		value := []byte(fmt.Sprintf(quotePattern, rollup.Timestamp, rollup.Open, rollup.Close, rollup.High, rollup.Low, rollup.Volume))
		err = s.db.Set(s.companyKey(exchange, cdq.Company, date), value)
		if err != nil {
			zap.L().Error("save company rollup failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", cdq.Company),
				zap.Time("date", date),
				zap.Any("rollup", rollup))
			return err
		}

		// save dividend
		if cdq.Dividend != nil && cdq.Dividend.Enable {
			err = s.saveCompanyDividend(exchange, cdq.Company, date, cdq.Dividend)
			if err != nil {
				return err
			}
		}

		// save split
		if cdq.Split != nil && cdq.Split.Enable {
			err = s.saveCompanySplit(exchange, cdq.Company, date, cdq.Split)
			if err != nil {
				return err
			}
		}

		// save pre
		if cdq.Pre != nil {
			err = s.saveCompanyQuoteSerial(exchange, cdq.Company, date, quotes.SerialTypePre, cdq.Pre)
			if err != nil {
				return err
			}
		}

		// save regular
		if cdq.Regular != nil {
			err = s.saveCompanyQuoteSerial(exchange, cdq.Company, date, quotes.SerialTypeRegular, cdq.Regular)
			if err != nil {
				return err
			}
		}

		// save post
		if cdq.Post != nil {
			err = s.saveCompanyQuoteSerial(exchange, cdq.Company, date, quotes.SerialTypePost, cdq.Post)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s Ledis) saveCompanyDividend(exchange exchanges.Exchange, company *quotes.Company, date time.Time, dividend *quotes.Dividend) error {
	value := []byte(fmt.Sprintf(dividendPattern, dividend.Timestamp, dividend.Amount))
	_, err := s.db.HSet(s.dividendKey(exchange, company), []byte(date.Format(datePattern)), value)
	if err != nil {
		zap.L().Error("save company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.Any("dividend", dividend))
		return err
	}

	return nil
}

func (s Ledis) saveCompanySplit(exchange exchanges.Exchange, company *quotes.Company, date time.Time, split *quotes.Split) error {
	value := []byte(fmt.Sprintf(splitPattern, split.Timestamp, split.Numerator, split.Denominator))
	_, err := s.db.HSet(s.splitKey(exchange, company), []byte(date.Format(datePattern)), value)
	if err != nil {
		zap.L().Error("save company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.Any("split", split))
		return err
	}

	return nil
}

func (s Ledis) saveCompanyQuoteSerial(exchange exchanges.Exchange, company *quotes.Company, date time.Time, serialType quotes.SerialType, serial *quotes.Serial) error {
	values := make([][]byte, 0, len(*serial))
	for _, quote := range *serial {
		values = append(values, []byte(fmt.Sprintf(quotePattern, quote.Timestamp, quote.Open, quote.Close, quote.High, quote.Low, quote.Volume)))
	}

	_, err := s.db.RPush(s.companySerialKey(exchange, company, date, serialType), values...)
	if err != nil {
		zap.L().Error("save company quote serial failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.Any("quotes", len(*serial)))
		return err
	}

	return nil
}

// Load load exchange daily quote
func (s Ledis) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	// load companies
	companies, err := s.loadCompanies(exchange, date)
	if err != nil {
		zap.L().Error("load exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	// load quotes
	cdqs, err := s.loadCompanyQuotes(exchange, date, companies)
	if err != nil {
		zap.L().Error("load company quotes failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	edq := &quotes.ExchangeDailyQuote{
		Exchange:  exchange.Code(),
		Date:      date,
		Companies: companies,
		Quotes:    cdqs,
	}

	return edq, nil
}

func (s Ledis) loadCompanies(exchange exchanges.Exchange, date time.Time) (map[string]*quotes.Company, error) {
	pairs, err := s.db.HGetAll(s.exchangeKey(exchange, date))
	if err != nil {
		zap.L().Error("load exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	// create company map
	companies := make(map[string]*quotes.Company)
	for _, pair := range pairs {
		parts := bytes.Split(pair.Value, []byte("|"))
		if len(parts) != 2 {
			if err != nil {
				zap.L().Error("decode exchange company failed",
					zap.Error(err),
					zap.String("exchange", exchange.Code()),
					zap.Time("date", date),
					zap.ByteString("company", pair.Value))
				return nil, err
			}
		}

		company := &quotes.Company{Code: string(parts[0]), Name: string(parts[1])}
		companies[company.Code] = company
	}

	return companies, nil
}

func (s Ledis) loadCompanyQuotes(exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) (map[string]*quotes.CompanyDailyQuote, error) {
	cdqs := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for companyCode, company := range companies {
		result, err := s.db.Exists(s.companyKey(exchange, company, date))
		if err != nil {
			zap.L().Error("load company failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		if result == 0 {
			continue
		}

		// load dividend
		dividend, err := s.loadCompanyDividend(exchange, company, date)
		if err != nil {
			return nil, err
		}

		// load split
		split, err := s.loadCompanySplit(exchange, company, date)
		if err != nil {
			return nil, err
		}

		// load pre
		pre, err := s.loadCompanyQuoteSerial(exchange, company, date, quotes.SerialTypePre)
		if err != nil {
			zap.L().Error("load company pre serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		// load regular
		regular, err := s.loadCompanyQuoteSerial(exchange, company, date, quotes.SerialTypeRegular)
		if err != nil {
			zap.L().Error("load company regular serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		// load post
		post, err := s.loadCompanyQuoteSerial(exchange, company, date, quotes.SerialTypePost)
		if err != nil {
			zap.L().Error("load company post serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		cdqs[companyCode] = &quotes.CompanyDailyQuote{
			Company:  company,
			Dividend: dividend,
			Split:    split,
			Pre:      pre,
			Regular:  regular,
			Post:     post,
		}
	}

	return cdqs, nil
}

func (s Ledis) loadCompanyDividend(exchange exchanges.Exchange, company *quotes.Company, date time.Time) (*quotes.Dividend, error) {
	value, err := s.db.HGet(s.dividendKey(exchange, company), []byte(date.Format(datePattern)))
	if err != nil {
		zap.L().Error("get company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil, err
	}

	dividend := &quotes.Dividend{Enable: false, Timestamp: 0, Amount: 0}
	if value == nil {
		// not exists
		return dividend, nil
	}

	_, err = fmt.Sscanf(string(value), dividendPattern, &dividend.Timestamp, &dividend.Amount)
	if err != nil {
		zap.L().Error("decode company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.ByteString("value", value))
		return nil, err
	}

	dividend.Enable = true
	return dividend, nil
}

func (s Ledis) loadCompanySplit(exchange exchanges.Exchange, company *quotes.Company, date time.Time) (*quotes.Split, error) {
	value, err := s.db.HGet(s.splitKey(exchange, company), []byte(date.Format(datePattern)))
	if err != nil {
		zap.L().Error("get company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil, err
	}

	split := &quotes.Split{Enable: false, Timestamp: 0, Numerator: 0, Denominator: 0}
	if value == nil {
		return split, nil
	}

	_, err = fmt.Sscanf(string(value), splitPattern, &split.Timestamp, &split.Numerator, &split.Denominator)
	if err != nil {
		zap.L().Error("decode company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.ByteString("value", value))
		return nil, err
	}

	split.Enable = true
	return split, nil
}

func (s Ledis) loadCompanyQuoteSerial(exchange exchanges.Exchange, company *quotes.Company, date time.Time, serialType quotes.SerialType) (*quotes.Serial, error) {
	values, err := s.db.LRange(s.companySerialKey(exchange, company, date, serialType), 0, -1)
	if err != nil {
		zap.L().Error("get company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil, err
	}

	serial := make([]quotes.Quote, len(values))
	for _, value := range values {
		var quote quotes.Quote
		_, err = fmt.Sscanf(string(value), quotePattern, &quote.Timestamp, &quote.Open, &quote.Close, &quote.High, &quote.Low, &quote.Volume)
		if err != nil {
			zap.L().Error("decode quote failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.ByteString("quote", value))
			return nil, err
		}

		serial = append(serial, quote)
	}
	qs := quotes.Serial(serial)

	return &qs, nil
}

func (s Ledis) remove(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	// remove company daily quotes
	var err error
	for _, cdq := range edq.Quotes {
		err = s.removeCompanyDailyQuote(exchange, date, cdq)
		if err != nil {
			zap.L().Error("remove company daily quote failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", cdq.Company),
				zap.Time("date", date))
			return err
		}
	}

	// remove exchange daily quote
	_, err = s.db.Del(s.exchangeKey(exchange, date))
	if err != nil {
		zap.L().Error("remove exchange daily companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

func (s Ledis) removeCompanyDailyQuote(exchange exchanges.Exchange, date time.Time, cdq *quotes.CompanyDailyQuote) error {
	// remove rollup
	_, err := s.db.Del(s.companyKey(exchange, cdq.Company, date))
	if err != nil {
		zap.L().Error("remove company rollup quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", cdq.Company),
			zap.Time("date", date))
		return err
	}

	// remove dividend
	_, err = s.db.HDel(s.dividendKey(exchange, cdq.Company), []byte(date.Format(datePattern)))
	if err != nil {
		zap.L().Error("remove company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", cdq.Company),
			zap.Time("date", date))
		return err
	}

	// remove split
	_, err = s.db.HDel(s.splitKey(exchange, cdq.Company), []byte(date.Format(datePattern)))
	if err != nil {
		zap.L().Error("remove company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", cdq.Company),
			zap.Time("date", date))
		return err
	}

	// remove pre
	_, err = s.db.Del(s.companySerialKey(exchange, cdq.Company, date, quotes.SerialTypePre))
	if err != nil {
		zap.L().Error("remove company pre serial failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", cdq.Company),
			zap.Time("date", date))
		return err
	}

	// remove regular
	_, err = s.db.Del(s.companySerialKey(exchange, cdq.Company, date, quotes.SerialTypeRegular))
	if err != nil {
		zap.L().Error("remove company regular serial failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", cdq.Company),
			zap.Time("date", date))
		return err
	}

	// remove post
	_, err = s.db.Del(s.companySerialKey(exchange, cdq.Company, date, quotes.SerialTypePost))
	if err != nil {
		zap.L().Error("remove company post serial failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", cdq.Company),
			zap.Time("date", date))
		return err
	}

	return nil
}
