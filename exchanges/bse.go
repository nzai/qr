package exchanges

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

func init() {
	Register(NewBse())
}

// Bse define beijing stock exchange
type Bse struct {
	location       *time.Location
	validCodeRegex *regexp.Regexp
}

// NewBse create beijing stock exchange
func NewBse() *Bse {
	location, _ := time.LoadLocation("Asia/Shanghai")
	return &Bse{
		location:       location,
		validCodeRegex: regexp.MustCompile(`\d{6}`),
	}
}

// Code get exchange code
func (s Bse) Code() string {
	return "Bse"
}

// Location get exchange location
func (s Bse) Location() *time.Location {
	return s.location
}

// Companies get exchange companies
func (s Bse) Companies() (map[string]*quotes.Company, error) {
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

func (s Bse) getCompanyByPageIndex(pageIndex int) (int, []*quotes.Company, error) {
	uv := url.Values{
		"page":      []string{strconv.Itoa(pageIndex)},
		"typejb":    []string{"T"},
		"xxfcbj[]":  []string{"2"},
		"sortfield": []string{"xxzqdm"},
		"sorttype":  []string{"asc"},
	}

	response, err := http.PostForm("https://www.bse.cn/nqxxController/nqxxCnzq.do?callback=jQuery331_1730808249531", uv)
	if err != nil {
		zap.S().Errorw("failed to get companies", "err", err, "page", pageIndex)
		return 0, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		zap.S().Errorw("failed to get companies", "statusCode", response.StatusCode)
		return 0, nil, fmt.Errorf("unexpected response status (%d)%s", response.StatusCode, http.StatusText(response.StatusCode))
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		zap.S().Errorw("failed to read response", "err", err, "page", pageIndex)
		return 0, nil, err
	}

	// remove jsonp prefix and suffix
	body = body[24 : len(body)-1]

	resp := new(bseCompanyResponse)
	err = json.Unmarshal(body, resp)
	if err != nil {
		zap.S().Errorw("unmarshal bse companies failed", "err", err, "body", body)
		return 0, nil, err
	}

	companies := make([]*quotes.Company, 0, len(resp.Content))
	for _, c := range resp.Content {
		if !s.validCodeRegex.MatchString(c.Xxzqdm) {
			continue
		}

		companies = append(companies, &quotes.Company{
			Code: c.Xxzqdm,
			Name: c.Xxzqjc,
		})
	}

	return resp.TotalPages, companies, nil
}

// Crawl company daily quote
func (s Bse) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	cdq := &quotes.CompanyDailyQuote{
		Company:  company,
		Dividend: &quotes.Dividend{Enable: false, Timestamp: 0, Amount: 0},
		Split:    &quotes.Split{Enable: false, Timestamp: 0, Numerator: 0, Denominator: 0},
		Pre:      new(quotes.Serial),
		Regular:  new(quotes.Serial),
		Post:     new(quotes.Serial),
	}

	if date.Before(utils.TodayZero(time.Now())) {
		return cdq, nil
	}

	// query quote date from bse
	u := fmt.Sprintf("https://www.bse.cn/companyEchartsController/getTimeSharingChart/list/%s.do?begin=0&end=-1", company.Code)

	code, buffer, err := utils.TryDownloadBytes(u, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		// zap.L().Warn("download yahoo finance quote failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	if code != http.StatusOK {
		if code != http.StatusNotFound {
			zap.L().Warn("download yahoo finance quote failed",
				zap.Error(err),
				zap.String("code", fmt.Sprintf("%d - %s", code, http.StatusText(code))),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.String("url", u))
		}

		return nil, fmt.Errorf("response status code %d", code)
	}

	// parse json
	quote := new(bseDailyQuoteResponse)
	err = json.Unmarshal(buffer, quote)
	if err != nil {
		zap.L().Error("unmarshal raw response json failed",
			zap.Error(err),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.ByteString("json", buffer))
		return nil, err
	}

	sort.Slice(quote.Data.Line, func(i, j int) bool {
		return quote.Data.Line[i].Hqgxsj < quote.Data.Line[j].Hqgxsj
	})

	var t time.Time
	var timeString string
	var lastQuote quotes.Quote
	for index, l := range quote.Data.Line {
		timeString = l.Hqjsrq + l.Hqgxsj[:4]
		t, err = time.Parse("200601021504", timeString)
		if err != nil {
			continue
		}

		quote := quotes.Quote{
			Timestamp: uint64(t.Unix()),
			Open:      lastQuote.Close,
			Close:     float32(l.Hqzjcj),
			High:      float32(l.Hqzjcj),
			Low:       float32(l.Hqzjcj),
			Volume:    uint64(l.Hqcjsl) - lastQuote.Volume,
		}

		if index == 0 {
			quote.Open = float32(l.Hqzrsp)
		}

		*cdq.Regular = append(*cdq.Regular, quote)
		lastQuote = quote
	}

	return cdq, nil
}

type bseCompanyResponse struct {
	Content          []*bseCompanyResponseContent `json:"content"`
	FirstPage        bool                         `json:"firstPage"`
	LastPage         bool                         `json:"lastPage"`
	Number           int                          `json:"number"`
	NumberOfElements int                          `json:"numberOfElements"`
	Size             int                          `json:"size"`
	Sort             any                          `json:"sort"`
	TotalElements    int                          `json:"totalElements"`
	TotalPages       int                          `json:"totalPages"`
}

type bseCompanyResponseContent struct {
	Fxssrq   string  `json:"fxssrq"`
	Xxbldw   int     `json:"xxbldw"`
	Xxbnsy   float64 `json:"xxbnsy"`
	Xxcfgbz  string  `json:"xxcfgbz"`
	Xxcqcx   string  `json:"xxcqcx"`
	Xxcyhbjq string  `json:"xxcyhbjq"`
	Xxdqr    string  `json:"xxdqr"`
	Xxdtjg   float64 `json:"xxdtjg"`
	Xxdzdtjg int     `json:"xxdzdtjg"`
	Xxdzztjg float64 `json:"xxdzztjg"`
	Xxfcbj   string  `json:"xxfcbj"`
	Xxfxsgb  int     `json:"xxfxsgb"`
	Xxghfl   int     `json:"xxghfl"`
	Xxgprq   string  `json:"xxgprq"`
	Xxgxsj   string  `json:"xxgxsj"`
	Xxhbzl   string  `json:"xxhbzl"`
	Xxhxcs   int     `json:"xxhxcs"`
	Xxhyzl   string  `json:"xxhyzl"`
	Xxisin   string  `json:"xxisin"`
	Xxjczq   string  `json:"xxjczq"`
	Xxjgdw   float64 `json:"xxjgdw"`
	Xxjsfl   float64 `json:"xxjsfl"`
	Xxjsrq   string  `json:"xxjsrq"`
	Xxmbxl   int     `json:"xxmbxl"`
	Xxmgmz   int     `json:"xxmgmz"`
	Xxqtyw   string  `json:"xxqtyw"`
	Xxsbcs   int     `json:"xxsbcs"`
	Xxsldw   int     `json:"xxsldw"`
	Xxsnsy   float64 `json:"xxsnsy"`
	Xxssdq   string  `json:"xxssdq"`
	Xxtpbz   string  `json:"xxtpbz"`
	Xxwltp   string  `json:"xxwltp"`
	Xxxjxz   int     `json:"xxxjxz"`
	Xxyhsl   float64 `json:"xxyhsl"`
	Xxywjc   string  `json:"xxywjc"`
	Xxzbqs   string  `json:"xxzbqs"`
	Xxzgb    int     `json:"xxzgb"`
	Xxzhbl   int     `json:"xxzhbl"`
	Xxzqdm   string  `json:"xxzqdm"`
	Xxzqjb   string  `json:"xxzqjb"`
	Xxzqjc   string  `json:"xxzqjc"`
	Xxzqqxr  string  `json:"xxzqqxr"`
	Xxzrdw   int     `json:"xxzrdw"`
	Xxzrlx   string  `json:"xxzrlx"`
	Xxzrzt   string  `json:"xxzrzt"`
	Xxzsssl  int     `json:"xxzsssl"`
	Xxztjg   float64 `json:"xxztjg"`
	Xxzxsbsl int     `json:"xxzxsbsl"`
}

type bseDailyQuoteResponse struct {
	Data struct {
		CompanyCode string               `json:"companyCode"`
		Total       int                  `json:"total"`
		Line        []*bseDailyQuoteLine `json:"line"`
		End         int                  `json:"end"`
		Begin       int                  `json:"begin"`
	} `json:"data"`
	Status int    `json:"status"`
	Msg    string `json:"msg"`
}

type bseDailyQuoteLine struct {
	Hqzgcj int     `json:"HQZGCJ"` // 最高成交
	Hqgxsj string  `json:"HQGXSJ"` // 时间 format: hMMdd
	Hqzrsp float64 `json:"HQZRSP"` // 昨日收盘
	Hqjrkp float64 `json:"HQJRKP"` // 今日开盘
	Hqjsrq string  `json:"HQJSRQ"` // 日期
	Hqzdcj float64 `json:"HQZDCJ"` // 最低成交
	Hqcjsl int     `json:"HQCJSL"` // 累计成交数量
	Hqcjje float64 `json:"HQCJJE"` // 累计成交金额
	ID     string  `json:"id"`
	Xxzqdm string  `json:"XXZQDM"`
	Hqzjcj float64 `json:"HQZJCJ"` // 最近成交
}
