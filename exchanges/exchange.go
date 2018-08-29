package exchanges

import (
	"time"

	"github.com/nzai/qr/quotes"
)

// Exchange 交易所
type Exchange interface {
	Code() string
	Location() *time.Location
	Companies() ([]*quotes.Company, error)
	Crawl(*quotes.Company, time.Time) (*quotes.DailyQuote, error)
}

var dict = map[string]Exchange{
	"Nasdaq": NewNasdaq(),
	"Nyse":   NewNyse(),
	"Amex":   NewAmex(),
	"Sse":    NewSse(),
	"Szse":   NewSzse(),
	"Hkex":   NewHkex(),
}

// Get 获取code对应的交易所
func Get(code string) (Exchange, bool) {
	exchange, found := dict[code]
	return exchange, found
}
