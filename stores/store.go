package stores

import (
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
)

// Store 存储
type Store interface {
	// Exists 是否存在
	Exists(exchanges.Exchange, ...time.Time) ([]bool, error)
	// Save 保存
	Save(exchanges.Exchange, time.Time, quotes.Encoder) error
	// Load 读取
	Load(exchanges.Exchange, time.Time, quotes.Decoder) error
}
