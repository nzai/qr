package exchanges

import (
	"errors"
	"regexp"
	"time"

	"github.com/guotie/gogb2312"
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
	url := "http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1"

	// download html from sse
	html, err := netop.GetString(url, netop.Retry(constants.RetryCount, constants.RetryInterval))
	if err != nil {
		zap.L().Error("download szse companies failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	// encode html from gb2312 to utf-8
	html, err, _, _ = gogb2312.ConvertGB2312String(html)
	if err != nil {
		return nil, err
	}

	companies, err := s.parse(html)
	if err != nil {
		zap.L().Error("parse failed", zap.Error(err), zap.String("html", html))
		return nil, err
	}

	return companies, nil
}

// parse parse result html
func (s Szse) parse(html string) (map[string]*quotes.Company, error) {
	// match by regex
	regex := regexp.MustCompile(`\' ><td  align='center'  >(\d{6})</td><td  align='center'  >([^<]*?)</td>`)
	group := regex.FindAllStringSubmatch(html, -1)

	companies := make(map[string]*quotes.Company)
	for _, section := range group {
		// remove duplicated
		if _, found := dict[section[1]]; found {
			continue
		}

		companies[section[1]] = &quotes.Company{Code: section[1], Name: section[2]}
	}

	if len(companies) == 0 {
		return nil, errors.New("szse companies not found")
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