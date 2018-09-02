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

// Sse define shanghai stock exchange
type Sse struct {
	source sources.Source
	sd     sources.SplitDividendSource
}

// NewSse create shanghai stock exchange
func NewSse() *Sse {
	return &Sse{
		source: sources.NewYahooFinance(),
		sd:     sources.NewIFengFinance(),
	}
}

// Code get exchange code
func (s Sse) Code() string {
	return "Sse"
}

// Location get exchange location
func (s Sse) Location() *time.Location {
	location, _ := time.LoadLocation("Asia/Shanghai")
	return location
}

// Companies get exchange companies
func (s Sse) Companies() ([]*quotes.Company, error) {

	urls := []string{
		"http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1",
		"http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=2",
	}
	referer := "http://www.sse.com.cn/assortment/stock/list/share/"

	var list []*quotes.Company
	for _, url := range urls {

		// download html from sse
		text, err := netop.GetString(url, netop.Refer(referer), netop.Retry(constants.RetryCount, constants.RetryInterval))
		if err != nil {
			zap.L().Error("download sse companies failed", zap.Error(err), zap.String("url", url))
			return nil, err
		}

		companies, err := s.parse(text)
		if err != nil {
			zap.L().Error("parse failed", zap.Error(err), zap.String("text", text))
			return nil, err
		}

		list = append(list, companies...)
	}

	// sort companies by code
	sort.Sort(quotes.CompanyList(list))

	return list, nil
}

// parse parse result html
func (s Sse) parse(text string) ([]*quotes.Company, error) {

	// encode html from gb2312 to utf-8
	converted, err, _, _ := gogb2312.ConvertGB2312String(text)
	if err != nil {
		zap.L().Error("convert gb2312 failed", zap.Error(err))
		return nil, err
	}

	// match by regex
	regex := regexp.MustCompile(`(\d{6})	  (\S+)	  \d{6}	  \S+`)
	group := regex.FindAllStringSubmatch(converted, -1)

	var companies []*quotes.Company
	for _, section := range group {
		companies = append(companies, &quotes.Company{Code: section[1], Name: section[2]})
	}

	if len(companies) == 0 {
		return nil, errors.New("sse companies not found")
	}

	return companies, nil
}

// Crawl company daily quote
func (s Sse) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	// 分时数据从雅虎抓取
	cdq, err := s.source.Crawl(company, date, ".SS")
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
