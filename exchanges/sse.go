package exchanges

import (
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

func init() {
	Register(NewSse())
}

// Sse define shanghai stock exchange
type Sse struct {
	source         sources.Source
	location       *time.Location
	sd             sources.SplitDividendSource
	validCodeRegex *regexp.Regexp
}

// NewSse create shanghai stock exchange
func NewSse() *Sse {
	location, _ := time.LoadLocation("Asia/Shanghai")
	return &Sse{
		source:         sources.NewYahooFinance(),
		location:       location,
		sd:             sources.NewIFengFinance(),
		validCodeRegex: regexp.MustCompile(`\d{6}`),
	}
}

// Code get exchange code
func (s Sse) Code() string {
	return "Sse"
}

// Location get exchange location
func (s Sse) Location() *time.Location {
	return s.location
}

// Companies get exchange companies
func (s Sse) Companies() (map[string]*quotes.Company, error) {
	companies := make(map[string]*quotes.Company)
	pageIndex := 1
	for {
		totalPage, _companies, err := s.getCompanyByPageIndex(pageIndex)
		if err != nil {
			return nil, err
		}

		for _, company := range _companies {
			companies[company.Code] = company
		}

		pageIndex++
		if pageIndex > totalPage {
			break
		}
	}

	return companies, nil
}

func (s Sse) getCompanyByPageIndex(pageIndex int) (int, []*quotes.Company, error) {
	pi := strconv.Itoa(pageIndex)
	now := strconv.FormatInt(time.Now().Unix()*1000, 10)
	url := os.Expand("http://query.sse.com.cn/sseQuery/commonQuery.do?jsonCallBack=jsonpCallback67704651&STOCK_TYPE=1&REG_PROVINCE=&CSRC_CODE=&STOCK_CODE=&sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L&COMPANY_STATUS=2%2C4%2C5%2C7%2C8&type=inParams&isPagination=true&pageHelp.cacheSize=1&pageHelp.beginPage=${index}&pageHelp.pageSize=100&pageHelp.pageNo=${index}&pageHelp.endPage=${index}&_=${now}",
		func(s string) string {
			switch s {
			case "index":
				return pi
			case "now":
				return now
			default:
				return s
			}
		})

	header := map[string]string{
		"Referer": "http://www.sse.com.cn/",
	}
	// download excel from sse
	_, html, err := utils.TryDownloadBytesWithHeader(url, header, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download sse companies failed", zap.Error(err), zap.String("url", url))
		return 0, nil, err
	}

	// remove jsonp prefix and suffix
	html = html[22 : len(html)-1]

	response := new(sseResponse)
	err = json.Unmarshal(html, response)
	if err != nil {
		zap.L().Error("unmarshal sse companies failed",
			zap.Error(err),
			zap.String("url", url),
			zap.ByteString("html", html))
		return 0, nil, err
	}

	companies := make([]*quotes.Company, 0, len(response.PageHelp.Data))
	for _, data := range response.PageHelp.Data {
		if !s.validCodeRegex.MatchString(data.COMPANYCODE) {
			continue
		}

		companies = append(companies, &quotes.Company{
			Code: data.COMPANYCODE,
			Name: data.SECNAMECN,
		})
	}

	return response.PageHelp.PageCount, companies, nil
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
		// zap.L().Error("query split and dividend failed",
		// 	zap.Error(err),
		// 	zap.Any("company", company),
		// 	zap.Time("date", date))
		return nil, err
	}

	cdq.Dividend = dividend
	cdq.Split = split

	return cdq, nil
}

type sseResponse struct {
	PageHelp struct {
		BeginPage int `json:"beginPage"`
		CacheSize int `json:"cacheSize"`
		Data      []struct {
			BSTOCKCODE    string `json:"B_STOCK_CODE"`
			COMPANYABBREN string `json:"COMPANY_ABBR_EN"`
			LISTDATE      string `json:"LIST_DATE"`
			LISTBOARD     string `json:"LIST_BOARD"`
			COMPANYABBR   string `json:"COMPANY_ABBR"`
			ASTOCKCODE    string `json:"A_STOCK_CODE"`
			DELISTDATE    string `json:"DELIST_DATE"`
			NUM           string `json:"NUM"`
			COMPANYCODE   string `json:"COMPANY_CODE"`
			SECNAMECN     string `json:"SEC_NAME_CN"`
			SECNAMEFULL   string `json:"SEC_NAME_FULL"`
		} `json:"data"`
		EndPage   int `json:"endPage"`
		PageCount int `json:"pageCount"`
		PageNo    int `json:"pageNo"`
		PageSize  int `json:"pageSize"`
		Total     int `json:"total"`
	} `json:"pageHelp"`
}
