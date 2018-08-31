package sources

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/nzai/netop"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// YahooFinance yahoo finance source
type YahooFinance struct{}

// NewYahooFinance create yahoo finance source
func NewYahooFinance() *YahooFinance {
	return &YahooFinance{}
}

// Crawl crawl company daily quote
func (yahoo YahooFinance) Crawl(company *quotes.Company, date time.Time, suffix string) (*quotes.DailyQuote, error) {

	tomorrow := date.AddDate(0, 0, 1)
	pattern := "https://query2.finance.yahoo.com/v8/finance/chart/%s%s?period2=%d&period1=%d&interval=1m&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%%7Csplit%%7Cearn&corsDomain=finance.yahoo.com"
	url := fmt.Sprintf(pattern, company.Code, suffix, tomorrow.Unix(), date.Unix())

	// query quote date from yahoo api
	response, err := netop.Get(url, netop.Retry(constants.RetryCount, constants.RetryInterval))
	if err != nil {
		zap.L().Error("download yahoo finance quote failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response status code %d", response.StatusCode)
	}

	buffer, err := ioutil.ReadAll(response.Body)
	if err != nil {
		zap.L().Error("read response body failed", zap.Error(err), zap.String("url", url))
		return nil, err
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
			zap.L().Info("ignore parse raw response due to symbol not found",
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
