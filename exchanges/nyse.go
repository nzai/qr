package exchanges

import (
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
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
	url := "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download"

	// download csv from nasdaq
	csv, err := utils.TryDownloadString(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download nyse companies failed", zap.Error(err), zap.String("url", url))
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
func (s Nyse) parseCSV(content string) (map[string]*quotes.Company, error) {
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
func (s Nyse) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
