package exchanges

import (
	"encoding/csv"
	"fmt"
	"sort"
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
	source sources.Source
}

// NewAmex create american stock exchange
func NewAmex() *Amex {
	return &Amex{sources.NewYahooFinance()}
}

// Code get exchange code
func (s Amex) Code() string {
	return "Amex"
}

// Location get exchange location
func (s Amex) Location() *time.Location {
	location, _ := time.LoadLocation("America/New_York")
	return location
}

// Companies get exchange companies
func (s Amex) Companies() ([]*quotes.Company, error) {

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

	// sort companies by code
	sort.Sort(quotes.CompanyList(companies))

	return companies, nil
}

// parseCSV parse result csv
func (s Amex) parseCSV(content string) ([]*quotes.Company, error) {

	reader := csv.NewReader(strings.NewReader(content))
	records, err := reader.ReadAll()
	if err != nil {
		zap.L().Error("read csv failed", zap.Error(err))
		return nil, err
	}

	dict := make(map[string]bool, 0)
	var companies []*quotes.Company
	for _, parts := range records[1:] {
		if len(parts) < 2 {
			zap.L().Error("csv format invalid", zap.Strings("parts", parts))
			return nil, fmt.Errorf("csv format invalid due to parts: %v", parts)
		}

		if strings.Contains(parts[0], "^") {
			continue
		}

		//	去重
		if _, found := dict[parts[0]]; found {
			continue
		}
		dict[parts[0]] = true

		companies = append(companies, &quotes.Company{
			Code: strings.TrimSpace(parts[0]),
			Name: strings.TrimSpace(parts[1]),
		})
	}

	return companies, nil
}

// Crawl company daily quote
func (s Amex) Crawl(company *quotes.Company, date time.Time) (*quotes.DailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
