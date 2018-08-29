package sources

import (
	"time"

	"github.com/nzai/qr/quotes"
)

// Source 数据源
type Source interface {
	// 获取公司每日报价
	Crawl(*quotes.Company, time.Time, string) (*quotes.DailyQuote, error)
}

// SplitDividendSource 查询拆股和除权的数据源
type SplitDividendSource interface {
	QuerySplitAndDividend(*quotes.Company, time.Time) (*quotes.Dividend, *quotes.Split, error)
}
