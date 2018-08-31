package stores

import (
	"fmt"
	"strings"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// Store define exchange daily quote store
type Store interface {
	// Exists exchange daily quote exists
	Exists(exchanges.Exchange, time.Time) (bool, error)
	// Save save exchange daily quote
	Save(exchanges.Exchange, time.Time, quotes.Encoder) error
	// Load load exchange daily quote
	Load(exchanges.Exchange, time.Time, quotes.Decoder) error
	// Remove remove exchange daily quote
	Remove(exchanges.Exchange, time.Time) error
}

// Parse parse command argument
func Parse(arg string) (Store, error) {
	parts := strings.Split(arg, ":")
	if len(parts) != 2 {
		zap.L().Error("store arg invalid", zap.String("arg", arg))
		return nil, fmt.Errorf("store arg invalid: %s", arg)
	}

	switch parts[0] {
	case "fs":
		return NewFileSystem(parts[1]), nil
	default:
		zap.L().Error("store type invalid", zap.String("type", parts[0]))
		return nil, fmt.Errorf("store type invalid: %s", parts[0])
	}
}
