package exchanges

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

// Hkex define hongkong exchange
type Hkex struct {
	source   sources.Source
	location *time.Location
}

// NewHkex create hongkong exchange
func NewHkex() *Hkex {
	location, _ := time.LoadLocation("Asia/Hong_Kong")
	return &Hkex{source: sources.NewYahooFinance(), location: location}
}

// Code get exchange code
func (s Hkex) Code() string {
	return "Hkex"
}

// Location get exchange location
func (s Hkex) Location() *time.Location {
	return s.location
}

// Companies get exchange companies
func (s Hkex) Companies() (map[string]*quotes.Company, error) {

	source := map[string]string{
		"http://www.hkex.com.hk/Market-Data/Securities-Prices/Equities?sc_lang=zh-HK":                      "https://www1.hkex.com.hk/hkexwidget/data/getequityfilter?lang=chi&token=%s&sort=5&order=0&all=1&qid=%d&callback=3322", // 股本證券
		"http://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products?sc_lang=zh-hk":      "https://www1.hkex.com.hk/hkexwidget/data/getetpfilter?lang=chi&token=%s&sort=2&order=1&all=1&qid=%d&callback=3322",    // 交易所買賣產品
		"http://www.hkex.com.hk/Market-Data/Securities-Prices/Derivative-Warrants?sc_lang=zh-hk":           "https://www1.hkex.com.hk/hkexwidget/data/getdwfilter?lang=chi&token=%s&sort=5&order=0&all=1&qid=%d&callback=3322",     // 衍生權證
		"http://www.hkex.com.hk/Market-Data/Securities-Prices/Callable-Bull-Bear-Contracts?sc_lang=zh-hk":  "https://www1.hkex.com.hk/hkexwidget/data/getcbbcfilter?lang=chi&token=%s&sort=5&order=0&all=1&qid=%d&callback=3322",   // 牛熊證
		"http://www.hkex.com.hk/Market-Data/Securities-Prices/Real-Estate-Investment-Trusts?sc_lang=zh-hk": "https://www1.hkex.com.hk/hkexwidget/data/getreitfilter?lang=chi&token=%s&sort=5&order=0&all=1&qid=%d&callback=3322",   // 房地產投資信託基金
		// "http://www.hkex.com.hk/Market-Data/Securities-Prices/Debt-Securities?sc_lang=zh-hk":               "https://www1.hkex.com.hk/hkexwidget/data/getdebtfilter?lang=chi&token=%s&sort=0&order=1&all=1&qid=%d&callback=3322",   // 債務證券
	}

	companies := make(map[string]*quotes.Company)
	for page, api := range source {
		_companies, err := s.queryCompanies(page, api)
		if err != nil {
			zap.L().Error("query hkex companies failed", zap.Error(err), zap.String("page", page), zap.String("api", api))
			return nil, err
		}

		for _, company := range _companies {
			// remove duplicated
			if _, found := companies[company.Code]; found {
				continue
			}

			companies[company.Code] = company
		}
	}

	return companies, nil
}

// queryCompanies query companies of special category
func (s Hkex) queryCompanies(page, api string) ([]*quotes.Company, error) {
	body, err := utils.TryDownloadString(page, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download hkex page failed", zap.Error(err), zap.String("url", page))
		return nil, err
	}

	regexToken := regexp.MustCompile(`\"Base64-AES-Encrypted-Token\";\s*?return \"([^\"]+?)\";`)

	matches := regexToken.FindStringSubmatch(body)
	if len(matches) != 2 {
		return nil, errors.New("get access token failed")
	}

	url := fmt.Sprintf(api, matches[1], time.Now().UnixNano())
	body, err = utils.TryDownloadString(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download hkex companies failed", zap.Error(err), zap.String("url", url), zap.String("token", matches[1]))
		return nil, err
	}

	regexCode := regexp.MustCompile(`\"ric\":\"(\d{2,5})\.HK\"\S+?\"nm\":\"([^\"]+)\"`)
	group := regexCode.FindAllStringSubmatch(body, -1)

	var companies []*quotes.Company
	for _, section := range group {
		companies = append(companies, &quotes.Company{Code: section[1], Name: section[2]})
	}

	return companies, nil
}

// Crawl company daily quote
func (s Hkex) Crawl(company *quotes.Company, date time.Time) (*quotes.CompanyDailyQuote, error) {
	return s.source.Crawl(company, date, ".HK")
}
