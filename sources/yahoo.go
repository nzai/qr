package sources

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nzai/go-utility/net"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// YahooFinance 雅虎财经数据源
type YahooFinance struct{}

// NewYahooFinance 新建雅虎财经数据源
func NewYahooFinance() *YahooFinance {
	return &YahooFinance{}
}

// Crawl 获取公司每天的报价
func (yahoo YahooFinance) Crawl(company *quotes.Company, date time.Time, suffix string) (*quotes.DailyQuote, error) {

	tomorrow := date.AddDate(0, 0, 1)
	pattern := "https://query2.finance.yahoo.com/v8/finance/chart/%s%s?period2=%d&period1=%d&interval=1m&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%%7Csplit%%7Cearn&corsDomain=finance.yahoo.com"
	url := fmt.Sprintf(pattern, company.Code, suffix, tomorrow.Unix(), date.Unix())

	// 查询Yahoo财经接口,返回股票分时数据
	yahooJSON, err := net.DownloadBufferRetry(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Warn("download raw response failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	// 解析Json
	quote := new(quotes.YahooQuote)
	err = json.Unmarshal(yahooJSON, quote)
	if err != nil {
		zap.L().Error("unmarshal raw response json failed",
			zap.Error(err),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("json", string(yahooJSON)))
		return nil, err
	}

	// 校验
	err = quote.Validate()
	if err != nil {
		if err == quotes.ErrYahooSymbolNotFound {
			zap.L().Info("ignore parse raw response due to symbol not found",
				zap.Error(err),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.String("json", string(yahooJSON)))
			// 雅虎没有这只股票的报价，放弃后续操作直接返回
			return nil, nil
		}

		zap.L().Error("yahoo quote validate failed",
			zap.Error(err),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("json", string(yahooJSON)))
		return nil, err
	}

	// 转换
	return quote.ToCompanyDailyQuote(uint64(date.Unix()), uint64(tomorrow.Unix())), nil
}
