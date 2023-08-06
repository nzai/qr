package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/utils"
	_ "github.com/taosdata/driver-go/v3/taosSql"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

type FetchData struct{}

func (f FetchData) Command() *cli.Command {
	return &cli.Command{
		Name:    "fetch",
		Aliases: []string{"f"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "url",
				Aliases:  []string{"u"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "tde-address",
				Aliases:  []string{"tde"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "tablename",
				Aliases:  []string{"table"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "exchange",
				Aliases:  []string{"e"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "symbol",
				Aliases:  []string{"s"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "type",
				Aliases:  []string{"t"},
				Required: true,
			},
			&cli.IntFlag{
				Name:     "save-batch-size",
				Aliases:  []string{"batch"},
				Value:    100,
				Required: false,
			},
		},
		Action: func(c *cli.Context) error {
			sourceURL := c.String("url")
			tdeAddress := c.String("tde-address")
			tableName := c.String("tablename")
			tagExchange := c.String("exchange")
			tagSymbol := c.String("symbol")
			tagType := c.String("type")
			batchSize := c.Int("batch")

			db, err := f.openDB(tdeAddress)
			if err != nil {
				return err
			}
			defer db.Close()

			data, err := f.fetch(sourceURL)
			if err != nil {
				return err
			}

			err = f.dropTable(c.Context, db, tableName)
			if err != nil {
				return err
			}

			err = f.save(c.Context, db, tableName, tagExchange, tagSymbol, tagType, data, batchSize)
			if err != nil {
				return err
			}

			return nil
		},
	}
}

func (f FetchData) openDB(address string) (*sql.DB, error) {
	db, err := sql.Open(tdeDriverName, address)
	if err != nil {
		zap.L().Error("connect tdengine failed", zap.Error(err), zap.String("address", address))
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		zap.L().Error("ping tdengine failed", zap.Error(err), zap.String("address", address))
		return nil, err
	}

	zap.L().Info("open database successfully", zap.String("address", address))

	return db, nil
}

func (f FetchData) fetch(url string) ([]*quotes.Quote, error) {
	header := map[string]string{
		"referer": "https://finance.yahoo.com/chart/AAPL",
	}
	// query yahoo finance
	code, buffer, err := utils.TryDownloadBytesWithHeader(url, header, 1, time.Second)
	if err != nil {
		zap.L().Warn("download yahoo finance quote failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	if code != http.StatusOK {
		return nil, fmt.Errorf("response status code %d", code)
	}

	// parse json
	quote := new(quotes.YahooQuote)
	err = json.Unmarshal(buffer, quote)
	if err != nil {
		zap.L().Error("unmarshal raw response json failed",
			zap.Error(err),
			zap.String("url", url),
			zap.ByteString("json", buffer))
		return nil, err
	}

	// validate response json
	err = quote.Validate()
	if err != nil {
		zap.L().Error("yahoo quote validate failed",
			zap.Error(err),
			zap.String("url", url),
			zap.ByteString("json", buffer))
		return nil, err
	}

	result := f.parse(quote)

	zap.L().Info("parse quotes successfully", zap.Int("quotes", len(result)))

	return result, nil
}

func (f FetchData) parse(q *quotes.YahooQuote) []*quotes.Quote {
	result := make([]*quotes.Quote, 0, len(q.Chart.Result[0].Timestamp))

	qs := q.Chart.Result[0].Indicators.Quotes[0]
	for index, ts := range q.Chart.Result[0].Timestamp {
		// ignore all zero quote
		if qs.Open[index] == 0 && qs.Close[index] == 0 && qs.High[index] == 0 && qs.Low[index] == 0 && qs.Volume[index] == 0 {
			continue
		}

		result = append(result, &quotes.Quote{
			Timestamp: uint64(ts),
			Open:      qs.Open[index],
			Close:     qs.Close[index],
			High:      qs.High[index],
			Low:       qs.Low[index],
			Volume:    uint64(qs.Volume[index]),
		})
	}

	return result
}

func (f FetchData) dropTable(ctx context.Context, db *sql.DB, tableName string) error {
	_, err := db.ExecContext(ctx, "drop table "+tableName)
	if err != nil {
		if err.Error() == "[0x362] Table does not exist" {
			return nil
		}

		zap.L().Error("drop table failed", zap.Error(err), zap.String("tableName", tableName))
		return err
	}

	zap.L().Info("drop table successfully", zap.String("tableName", tableName))

	return nil
}

func (f FetchData) save(ctx context.Context, db *sql.DB, tableName, tagExchange, tagSymbol, tagType string, data []*quotes.Quote, batchSize int) error {
	sb := new(strings.Builder)
	fmt.Fprintf(sb, "insert into %s using quotes tags('%s', '%s', '%s') values ",
		tableName,
		tagExchange,
		tagSymbol,
		tagType)

	var err error
	for index, quote := range data {
		fmt.Fprintf(sb, "(%d, %f, %f, %f, %f, %d) ", quote.Timestamp*1000, quote.Open, quote.Close, quote.High, quote.Low, quote.Volume)

		if index%batchSize == batchSize-1 || index == len(data)-1 {
			_, err = db.ExecContext(ctx, sb.String())
			if err != nil {
				zap.L().Error("save quotes failed", zap.Error(err), zap.String("sql", sb.String()))
				return err
			}

			zap.L().Info("save result successfully", zap.Int("count", index+1))

			sb.Reset()

			fmt.Fprintf(sb, "insert into %s using quotes tags('%s', '%s', '%s') values ",
				tableName,
				tagExchange,
				tagSymbol,
				tagType)
		}
	}

	return nil
}
