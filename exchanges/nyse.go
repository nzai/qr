package exchanges

import (
	"time"

	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
)

func init() {
	Register(NewNyse())
}

// Nyse define new york stock exchange
type Nyse struct {
	source        sources.Source
	companySource *sources.NasdaqSource
	location      *time.Location
}

// NewNyse create new york stock exchange
func NewNyse() *Nyse {
	location, _ := time.LoadLocation("America/New_York")
	return &Nyse{
		source:        sources.NewYahooFinance(),
		companySource: sources.NewNasdaqSource(),
		location:      location,
	}
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
	return s.companySource.Companies(s.Code())
}

// Crawl company daily quote
func (s Nyse) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
