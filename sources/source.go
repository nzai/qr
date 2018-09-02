package sources

import (
	"time"

	"github.com/nzai/qr/quotes"
)

// Source define company daily quote source
type Source interface {
	// Crawl company daily quote
	Crawl(*quotes.Company, time.Time, string) (*quotes.CompanyDailyQuote, error)
}

// SplitDividendSource define company daily split and dividend source
type SplitDividendSource interface {
	QuerySplitAndDividend(*quotes.Company, time.Time) (*quotes.Dividend, *quotes.Split, error)
}
