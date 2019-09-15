package exchanges

import (
	"fmt"
	"time"

	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
)

// Nyse define new york stock exchange
type Nyse struct {
	source   sources.Source
	location *time.Location
}

// NewNyse create new york stock exchange
func NewNyse() *Nyse {
	location, _ := time.LoadLocation("America/New_York")
	return &Nyse{source: sources.NewYahooFinance(), location: location}
}

// Code get exchange code
func (s Nyse) Code() string {
	return "Nyse"
}

// Location get exchange location
func (s Nyse) Location() *time.Location {
	return s.location
}

// Companies get exchange companies
func (s Nyse) Companies() (map[string]*quotes.Company, error) {
	urls := make([]string, 0, 26)
	for index := 0; index < 26; index++ {
		urls = append(urls, fmt.Sprintf("http://eoddata.com/stocklist/NYSE/%s.htm", string('A'+index)))
	}

	return getEodSymbol().Companies(urls...)
}

// Crawl company daily quote
func (s Nyse) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
