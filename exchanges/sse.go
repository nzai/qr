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

// Sse 上海证券交易所
type Sse struct {
	source sources.Source
}

// NewSse 新建上海证券交易所
func NewSse() *Sse {
	return &Sse{source: sources.NewYahooFinance()}
}

// Code 代码
func (s Sse) Code() string {
	return "Sse"
}

// Location 所处时区
func (s Sse) Location() *time.Location {
	location, _ := time.LoadLocation("Asia/Shanghai")
	return location
}

// Companies 上市公司
func (s Sse) Companies() ([]*quotes.Company, error) {

	urls := []string{
		"http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1",
		"http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=2",
	}
	referer := "http://www.sse.com.cn/assortment/stock/list/share/"

	var list []*quotes.Company
	for _, url := range urls {

		//	尝试从网络获取实时上市公司列表
		text, err := net.DownloadStringRefererRetry(url, referer, constants.RetryCount, constants.RetryInterval)
		if err != nil {
			zap.L().Error("download sse companies failed", zap.Error(err), zap.String("url", url))
			return nil, err
		}

		// 解析
		companies, err := s.parse(text)
		if err != nil {
			zap.L().Error("parse failed", zap.Error(err), zap.String("text", text))
			return nil, err
		}

		list = append(list, companies...)
	}

	// 按Code排序
	sort.Sort(quotes.CompanyList(list))

	return list, nil
}

// parse 解析
func (s Sse) parse(text string) ([]*quotes.Company, error) {

	//	深圳证券交易所的查询结果是GBK编码的，需要转成UTF8
	converted, err, _, _ := gogb2312.ConvertGB2312String(text)
	if err != nil {
		zap.L().Error("convert gb2312 failed", zap.Error(err))
		return nil, err
	}

	//  使用正则分析json
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

// Crawl 抓取
func (s Sse) Crawl(company *quotes.Company, date time.Time) (*quotes.DailyQuote, error) {
	// 分时数据从雅虎抓取
	return s.source.Crawl(company, date, ".SS")
}
