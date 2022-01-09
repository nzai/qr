package trade_system

import (
	"context"
)

type TradeSystem interface {
	Init(ctx context.Context) error
	Next(ctx *Context) error
	Close(ctx context.Context) error
}
