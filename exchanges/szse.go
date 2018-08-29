package exchanges

import (
	"errors"
	"regexp"
	"sort"
	"time"

	"github.com/guotie/gogb2312"
	"github.com/nzai/go-utility/net"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"go.uber.org/zap"
)

// Szse 深圳证券交易所
type Szse struct {
	source sources.Source
}

// NewSzse 新建深圳证券交易所
func NewSzse() *Szse {
	return &Szse{source: sources.NewYahooFinance()}
}

// Code 代码
func (s Szse) Code() string {
	return "Szse"
}

// Location 所处时区
func (s Szse) Location() *time.Location {
	location, _ := time.LoadLocation("Asia/Shanghai")
	return location
}

// Companies 上市公司
func (s Szse) Companies() ([]*quotes.Company, error) {

	url := "http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1"

	// 尝试从网络获取实时上市公司列表
	html, err := net.DownloadStringRetry(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download szse companies failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	// 深圳证券交易所的查询结果是GBK编码的，需要转成UTF8
	html, err, _, _ = gogb2312.ConvertGB2312String(html)
	if err != nil {
		return nil, err
	}

	// 解析
	companies, err := s.parse(html)
	if err != nil {
		zap.L().Error("parse failed", zap.Error(err), zap.String("html", html))
		return nil, err
	}

	// 按Code排序
	sort.Sort(quotes.CompanyList(companies))

	return companies, nil
}

// parse 解析
func (s Szse) parse(html string) ([]*quotes.Company, error) {

	//  使用正则分析html
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

// Crawl 抓取
func (s Szse) Crawl(company *quotes.Company, date time.Time) (*quotes.DailyQuote, error) {
	// 分时数据从雅虎抓取
	return s.source.Crawl(company, date, ".SZ")
}
