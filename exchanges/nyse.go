package exchanges

import (
	"encoding/csv"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nzai/go-utility/net"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"go.uber.org/zap"
)

// Nyse 纽约证券交易所
type Nyse struct {
	source sources.Source
}

// NewNyse 新建纽约证券交易所
func NewNyse() *Nyse {
	return &Nyse{sources.NewYahooFinance()}
}

// Code 代码
func (s Nyse) Code() string {
	return "Nyse"
}

// Location 所处时区
func (s Nyse) Location() *time.Location {
	location, _ := time.LoadLocation("America/New_York")
	return location
}

// Companies 上市公司
func (s Nyse) Companies() ([]*quotes.Company, error) {

	url := "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download"

	//	尝试从网络获取实时上市公司列表
	csv, err := net.DownloadStringRetry(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download nyse companies failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	// 解析CSV
	companies, err := s.parseCSV(csv)
	if err != nil {
		zap.L().Error("parse csv failed", zap.Error(err), zap.String("csv", csv))
		return nil, err
	}

	// 按Code排序
	sort.Sort(quotes.CompanyList(companies))

	return companies, nil
}

// parseCSV 解析CSV
func (s Nyse) parseCSV(content string) ([]*quotes.Company, error) {

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

// Crawl 抓取
func (s Nyse) Crawl(company *quotes.Company, date time.Time) (*quotes.DailyQuote, error) {
	return s.source.Crawl(company, date, "")
}
