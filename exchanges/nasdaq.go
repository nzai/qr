package exchanges

import (
	"fmt"
	"time"

	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
)

// Nasdaq define nasdaq exchange
type Nasdaq struct {
	source   sources.Source
	location *time.Location
}

// NewNasdaq create nasdaq exchange
func NewNasdaq() *Nasdaq {
	location, _ := time.LoadLocation("America/New_York")
	return &Nasdaq{source: sources.NewYahooFinance(), location: location}
}

// Code get exchange code
func (s Nasdaq) Code() string {
	return "Nasdaq"
}

// Location get exchange location
func (s Nasdaq) Location() *time.Location {
	return s.location
}

// Companies get exchange companies
func (s Nasdaq) Companies() (map[string]*quotes.Company, error) {
	urls := make([]string, 0, 26)
	for index := 0; index < 26; index++ {
		urls = append(urls, fmt.Sprintf("http://eoddata.com/stocklist/NASDAQ/%s.htm", string('A'+index)))
	}

	return getEodSymbol().Companies(urls...)
}

// Crawl company daily quote
func (s Nasdaq) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
