package exchanges

import (
	"fmt"
	"time"

	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
)

// Amex define american stock exchange
type Amex struct {
	source   sources.Source
	location *time.Location
}

// NewAmex create american stock exchange
func NewAmex() *Amex {
	location, _ := time.LoadLocation("America/New_York")
	return &Amex{source: sources.NewYahooFinance(), location: location}
}

// Code get exchange code
func (s Amex) Code() string {
	return "Amex"
}

// Location get exchange location
func (s Amex) Location() *time.Location {
	return s.location
}

// Companies get exchange companies
func (s Amex) Companies() (map[string]*quotes.Company, error) {
	urls := make([]string, 0, 26)
	for index := 0; index < 26; index++ {
		urls = append(urls, fmt.Sprintf("http://eoddata.com/stocklist/AMEX/%s.htm", string('A'+index)))
	}

	return getEodSymbol().Companies(urls...)
}

// Crawl company daily quote
func (s Amex) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
