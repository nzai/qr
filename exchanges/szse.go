package exchanges

import (
	"bytes"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/nzai/netop"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"go.uber.org/zap"
)

// Szse define shenzhen stock exchange
type Szse struct {
	source   sources.Source
	location *time.Location
	sd       sources.SplitDividendSource
}

// NewSzse create shenzhen stock exchange
func NewSzse() *Szse {
	location, _ := time.LoadLocation("Asia/Shanghai")
	return &Szse{
		source:   sources.NewYahooFinance(),
		location: location,
		sd:       sources.NewIFengFinance(),
	}
}

// Code get exchange code
func (s Szse) Code() string {
	return "Szse"
}

// Location get exchange location
func (s Szse) Location() *time.Location {
	return s.location
}

// Companies get exchange companies
func (s Szse) Companies() (map[string]*quotes.Company, error) {
	urls := map[string][]int{
		"http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.49987789273726513": []int{5},
		"http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab2&random=0.42963499040546527": []int{10},
		"http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab3&random=0.9988466864844461":  []int{5, 10},
	}

	companies := make(map[string]*quotes.Company)
	for url, columns := range urls {
		_companies, err := s.getByURL(url, columns)
		if err != nil {
			return nil, err
		}

		var found bool
		for code, company := range _companies {
			_, found = companies[code]
			if found {
				continue
			}

			companies[code] = company
		}
	}

	return companies, nil
}

func (s Szse) getByURL(url string, columns []int) (map[string]*quotes.Company, error) {
	// download html from sse
	html, err := netop.GetBytes(url, netop.Retry(constants.RetryCount, constants.RetryInterval))
	if err != nil {
		zap.L().Error("download szse companies failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	xlsx, err := excelize.OpenReader(bytes.NewBuffer(html))
	if err != nil {
		zap.L().Error("download szse companies failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	sheetName := xlsx.GetSheetName(1)
	rows := xlsx.GetRows(sheetName)

	companies := make(map[string]*quotes.Company)
	for _, row := range rows {
		for _, column := range columns {
			if column+1 >= len(row) {
				continue
			}

			_, found := companies[row[column]]
			if found {
				continue
			}

			companies[row[column]] = &quotes.Company{
				Code: row[column],
				Name: row[column+1],
			}
		}
	}

	return companies, nil
}

// Crawl company daily quote
func (s Szse) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	// 分时数据从雅虎抓取
	cdq, err := s.source.Crawl(company, date, ".SZ")
	if err != nil {
		return nil, err
	}

	// 因为雅虎财经api中关于上海和深证交易所的股票拆分/送股信息是错误的，所以分红配股单独查询
	dividend, split, err := s.sd.QuerySplitAndDividend(company, date)
	if err != nil {
		zap.L().Error("query split and dividend failed",
			zap.Error(err),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil, err
	}

	cdq.Dividend = dividend
	cdq.Split = split

	return cdq, nil
}
