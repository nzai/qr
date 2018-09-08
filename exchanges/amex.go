package exchanges

import (
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	"github.com/nzai/netop"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"go.uber.org/zap"
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
	url := "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"

	// download csv from nasdaq
	csv, err := netop.GetString(url, netop.Retry(constants.RetryCount, constants.RetryInterval))
	if err != nil {
		zap.L().Error("download amex companies failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	companies, err := s.parseCSV(csv)
	if err != nil {
		zap.L().Error("parse csv failed", zap.Error(err), zap.String("csv", csv))
		return nil, err
	}

	return companies, nil
}

// parseCSV parse result csv
func (s Amex) parseCSV(content string) (map[string]*quotes.Company, error) {
	reader := csv.NewReader(strings.NewReader(content))
	records, err := reader.ReadAll()
	if err != nil {
		zap.L().Error("read csv failed", zap.Error(err))
		return nil, err
	}

	companies := make(map[string]*quotes.Company)
	for _, parts := range records[1:] {
		if len(parts) < 2 {
			zap.L().Error("csv format invalid", zap.Strings("parts", parts))
			return nil, fmt.Errorf("csv format invalid due to parts: %v", parts)
		}

		if strings.Contains(parts[0], "^") {
			continue
		}

		// remove duplicated
		if _, found := companies[strings.TrimSpace(parts[0])]; found {
			continue
		}

		companies[strings.TrimSpace(parts[0])] = &quotes.Company{
			Code: strings.TrimSpace(parts[0]),
			Name: strings.TrimSpace(parts[1]),
		}
	}

	return companies, nil
}

// Crawl company daily quote
func (s Amex) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
