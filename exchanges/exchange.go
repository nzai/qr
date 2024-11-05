package exchanges

import (
	"fmt"
	"strings"
	"time"

	"github.com/nzai/qr/quotes"
)

// Exchange define exchanges
type Exchange interface {
	Code() string
	Location() *time.Location
	Companies() (map[string]*quotes.Company, error)
	Crawl(*quotes.Company, time.Time) (*quotes.CompanyDailyQuote, error)
}

var _exchanges = map[string]Exchange{}

func Register(e Exchange) {
	_exchanges[e.Code()] = e
}

// Get get exchange by code
func Get(code string) (Exchange, bool) {
	exchange, found := _exchanges[code]
	return exchange, found
}

// Parse parse command argument
func Parse(arg string) ([]Exchange, error) {
	parts := strings.Split(arg, ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("exchange arg invalid: %s", arg)
	}

	exchanges := make([]Exchange, 0, len(parts))
	for _, code := range parts {
		exchange, found := Get(code)
		if !found {
			return nil, fmt.Errorf("invalid exchange: %s", code)
		}

		exchanges = append(exchanges, exchange)
	}

	return exchanges, nil
}
