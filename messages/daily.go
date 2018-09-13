package messages

import (
	"time"

	"github.com/nzai/qr/quotes"
)

// CompanyDaily 公司每日消息
type CompanyDaily struct {
	Exchange string
	Company  *quotes.Company
	Date     time.Time
}
