package exchanges

import (
	"regexp"
	"sync"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

var (
	_eodOnce  sync.Once
	_eodLogic *eodSymbol
)

// eodSymbol eoddata symbol list
type eodSymbol struct {
	regexp *regexp.Regexp
}

// GeteodSymbol get eod logic
func getEodSymbol() *eodSymbol {
	_eodOnce.Do(func() {
		_eodLogic = &eodSymbol{
			regexp: regexp.MustCompile(`<td><A href[^>]+?>(\w+)<\/A><\/td><td>([^<]+?)<`),
		}
	})
	return _eodLogic
}

func (s eodSymbol) Companies(urls ...string) (map[string]*quotes.Company, error) {
	companies := make(map[string]*quotes.Company)
	for _, url := range urls {
		_companies, err := s.getPageCompanies(url)
		if err != nil {
			return nil, err
		}

		for _, company := range _companies {
			companies[company.Code] = company
		}
	}

	return companies, nil
}

func (s eodSymbol) getPageCompanies(url string) ([]*quotes.Company, error) {
	// download csv from nasdaq
	html, err := utils.TryDownloadString(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download eoddata company symbol list failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	matches := s.regexp.FindAllStringSubmatch(html, -1)
	companies := make([]*quotes.Company, 0, len(matches))
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		companies = append(companies, &quotes.Company{Code: match[1], Name: match[2]})
	}

	return companies, nil
}
