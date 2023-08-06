package exchanges

import (
	"time"

	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
)

// Amex define american stock exchange
type Amex struct {
	source        sources.Source
	companySource *sources.NasdaqSource
	location      *time.Location
}

// NewAmex create american stock exchange
func NewAmex() *Amex {
	location, _ := time.LoadLocation("America/New_York")
	return &Amex{
		source:        sources.NewYahooFinance(),
		companySource: sources.NewNasdaqSource(),
		location:      location,
	}
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
	return s.companySource.Companies(s.Code())
}

// Crawl company daily quote
func (s Amex) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
