package exchanges

import (
	"errors"
	"regexp"
	"sort"
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
	source sources.Source
}

// NewSzse create shenzhen stock exchange
func NewSzse() *Szse {
	return &Szse{source: sources.NewYahooFinance()}
}

// Code get exchange code
func (s Szse) Code() string {
	return "Szse"
}

// Location get exchange location
func (s Szse) Location() *time.Location {
	location, _ := time.LoadLocation("Asia/Shanghai")
	return location
}

// Companies get exchange companies
func (s Szse) Companies() ([]*quotes.Company, error) {

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

	// sort companies by code
	sort.Sort(quotes.CompanyList(companies))

	return companies, nil
}

// parse parse result html
func (s Szse) parse(html string) ([]*quotes.Company, error) {

	// match by regex
	regex := regexp.MustCompile(`\' ><td  align='center'  >(\d{6})</td><td  align='center'  >([^<]*?)</td>`)
	group := regex.FindAllStringSubmatch(html, -1)

	var companies []*quotes.Company
	for _, section := range group {
		companies = append(companies, &quotes.Company{Code: section[1], Name: section[2]})
	}

	if len(companies) == 0 {
		return nil, errors.New("szse companies not found")
	}

	return companies, nil
}

// Crawl company daily quote
func (s Szse) Crawl(company *quotes.Company, date time.Time) (*quotes.DailyQuote, error) {
	return s.source.Crawl(company, date, ".SZ")
}
