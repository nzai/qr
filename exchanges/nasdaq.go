package exchanges

import (
	"time"

	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
)

func init() {
	Register(NewNasdaq())
}

// Nasdaq define nasdaq exchange
type Nasdaq struct {
	source        sources.Source
	companySource *sources.NasdaqSource
	location      *time.Location
}

// NewNasdaq create nasdaq exchange
func NewNasdaq() *Nasdaq {
	location, _ := time.LoadLocation("America/New_York")
	return &Nasdaq{
		source:        sources.NewYahooFinance(),
		companySource: sources.NewNasdaqSource(),
		location:      location,
	}
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
	return s.companySource.Companies(s.Code())
}

// Crawl company daily quote
func (s Nasdaq) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
