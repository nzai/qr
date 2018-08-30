package sources

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/nzai/netop"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// IFengFinance 凤凰财经数据源
type IFengFinance struct {
	pattern *regexp.Regexp
}

// NewIFengFinance 新建凤凰财经数据源
func NewIFengFinance() *IFengFinance {
	pattern := regexp.MustCompile(`每10股现金\(税后\)<\/td>\s*?<td>([\d\.]+?)元<\/td>[\s\S]*?每10股送红股<\/td>\s*?<td>([\d\.]+?)股<\/td>[\s\S]*?每10股转增股本<\/td>\s*?<td>([\d\.]+?)股<\/td>[\s\S]*?除权除息日<\/td>\s*?<td>(\S*?)<\/td>`)
	return &IFengFinance{pattern}
}

// QuerySplitAndDividend 查询拆股和除权
func (s IFengFinance) QuerySplitAndDividend(company *quotes.Company, date time.Time) (*quotes.Dividend, *quotes.Split, error) {
	url := fmt.Sprintf("http://app.finance.ifeng.com/data/stock/tab_fhpxjl.php?symbol=%s", company.Code)

	// 查询凤凰财经数据接口,返回分红配股信息
	response, err := netop.Get(url, netop.Retry(constants.RetryCount, constants.RetryInterval))
	if err != nil {
		zap.L().Warn("download dividend and split failed", zap.Error(err), zap.String("url", url))
		return nil, nil, err
	}
	defer response.Body.Close()

	// ignore on server forbidden
	if response.StatusCode == http.StatusForbidden {
		return nil, nil, nil
	}

	buffer, err := ioutil.ReadAll(response.Body)
	if err != nil {
		zap.L().Error("download dividend and split failed", zap.Error(err), zap.String("url", url))
		return nil, nil, err
	}

	dateText := date.Format("2006-01-02")
	for _, matches := range s.pattern.FindAllStringSubmatch(string(buffer), -1) {
		if len(matches) != 5 {
			zap.L().Error("ifeng finance html match count invalid",
				zap.Any("matches", matches),
				zap.ByteString("html", buffer))
			return nil, nil, fmt.Errorf("ifeng finance html match count invalid due to matches count: %d", len(matches))
		}

		// 只需要指定日期的
		if matches[4] != dateText {
			continue
		}

		// 派息
		amount, err := strconv.ParseFloat(matches[1], 32)
		if err != nil {
			zap.L().Error("ifeng finance dividend invalid", zap.String("dividend", matches[3]))
			return nil, nil, fmt.Errorf("ifeng finance dividend invalid due to dividend: %s", matches[3])
		}

		dividend := &quotes.Dividend{
			Enable:    amount > 0,
			Timestamp: uint64(date.Unix()),
			Amount:    float32(amount / 10), // A股都是按每十股计算
		}

		// 送股
		numerator1, err := strconv.ParseFloat(matches[2], 32)
		if err != nil {
			zap.L().Error("ifeng finance split numerator invalid", zap.String("numerator", matches[1]))
			return nil, nil, fmt.Errorf("ifeng finance split numerator invalid due to numerator: %s", matches[1])
		}

		// 转增
		numerator2, err := strconv.ParseFloat(matches[3], 32)
		if err != nil {
			zap.L().Error("ifeng finance split numerator invalid", zap.String("numerator", matches[2]))
			return nil, nil, fmt.Errorf("ifeng finance split numerator invalid due to numerator: %s", matches[2])
		}

		split := &quotes.Split{
			Enable:      numerator1+numerator2 > 0,
			Timestamp:   uint64(date.Unix()),
			Numerator:   float32(numerator1 + numerator2),
			Denominator: 10, // A股都是按每十股计算
		}

		return dividend, split, nil
	}

	return new(quotes.Dividend), new(quotes.Split), nil
}
