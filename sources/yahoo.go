package sources

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

// YahooFinance yahoo finance source
type YahooFinance struct{}

// NewYahooFinance create yahoo finance source
func NewYahooFinance() *YahooFinance {
	return &YahooFinance{}
}

// Crawl crawl company daily quote
func (yahoo YahooFinance) Crawl(company *quotes.Company, date time.Time, suffix string) (*quotes.CompanyDailyQuote, error) {
	tomorrow := date.AddDate(0, 0, 1)
	symbol := company.Code + suffix
	pattern := "https://query2.finance.yahoo.com/v8/finance/chart/%s?symbol=%s&period1=%d&period2=%d&interval=1m&includePrePost=true&events=div|split|earn&corsDomain=finance.yahoo.com"
	url := fmt.Sprintf(pattern, symbol, symbol, date.Unix(), tomorrow.Unix())

	// query quote date from yahoo api
	code, buffer, err := utils.TryDownloadBytes(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		// zap.L().Warn("download yahoo finance quote failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	if code != http.StatusOK {
		if code != http.StatusNotFound {
			zap.L().Warn("download yahoo finance quote failed",
				zap.Error(err),
				zap.String("code", fmt.Sprintf("%d - %s", code, http.StatusText(code))),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.String("url", url))
		}

		return nil, fmt.Errorf("response status code %d", code)
	}

	// parse json
	quote := new(quotes.YahooQuote)
	err = json.Unmarshal(buffer, quote)
	if err != nil {
		zap.L().Error("unmarshal raw response json failed",
			zap.Error(err),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.ByteString("json", buffer))
		return nil, err
	}

	// validate response json
	err = quote.Validate()
	if err != nil {
		if err == quotes.ErrYahooSymbolNotFound {
			// ignore unknown symblo
			zap.L().Debug("ignore parse raw response due to symbol not found",
				zap.Error(err),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.ByteString("json", buffer))
			return nil, nil
		}

		zap.L().Error("yahoo quote validate failed",
			zap.Error(err),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.ByteString("json", buffer))
		return nil, err
	}

	return quote.ToCompanyDailyQuote(company, uint64(date.Unix()), uint64(tomorrow.Unix())), nil
}
