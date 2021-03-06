package sources

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/utils"
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
	code, buffer, err := utils.TryDownloadBytes(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Warn("download dividend and split failed", zap.Error(err), zap.String("url", url))
		return nil, nil, err
	}

	dividend := &quotes.Dividend{Enable: false, Timestamp: 0, Amount: 0}
	split := &quotes.Split{Enable: false, Timestamp: 0, Numerator: 0, Denominator: 0}

	// ignore on server forbidden
	if code == http.StatusForbidden {
		return dividend, split, nil
	}

	if code != http.StatusOK {
		zap.L().Warn("unexpected response status", zap.Int("code", code))
		return nil, nil, fmt.Errorf("unexpected response status (%d)%s", code, http.StatusText(code))
	}

	dateText := date.Format("2006-01-02")
	for _, matches := range s.pattern.FindAllStringSubmatch(string(buffer), -1) {
		if len(matches) != 5 {
			zap.L().Warn("ifeng finance html match count invalid",
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
			zap.L().Warn("ifeng finance dividend invalid", zap.String("dividend", matches[3]))
			return nil, nil, fmt.Errorf("ifeng finance dividend invalid due to dividend: %s", matches[3])
		}

		if amount > 0 {
			dividend.Enable = true
			dividend.Timestamp = uint64(date.Unix())
			dividend.Amount = float32(amount / 10) // A股都是按每十股计算
		}

		// 送股
		numerator1, err := strconv.ParseFloat(matches[2], 32)
		if err != nil {
			zap.L().Warn("ifeng finance split numerator invalid", zap.String("numerator", matches[1]))
			return nil, nil, fmt.Errorf("ifeng finance split numerator invalid due to numerator: %s", matches[1])
		}

		// 转增
		numerator2, err := strconv.ParseFloat(matches[3], 32)
		if err != nil {
			zap.L().Warn("ifeng finance split numerator invalid", zap.String("numerator", matches[2]))
			return nil, nil, fmt.Errorf("ifeng finance split numerator invalid due to numerator: %s", matches[2])
		}

		if numerator1+numerator2 > 0 {
			split.Enable = true
			split.Timestamp = uint64(date.Unix())
			split.Numerator = float32(numerator1 + numerator2)
			split.Denominator = 10 // A股都是按每十股计算
		}

		break
	}

	return dividend, split, nil
}
