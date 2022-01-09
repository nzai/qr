package trade_system

import (
	"context"
	"errors"
	"sync"

	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

var (
	ErrNotEnoughBalance   = errors.New("not enough balance")
	ErrPriceOutOfRange    = errors.New("price out of range")
	ErrQuantityOutOfRange = errors.New("quantity out of range")
)

type Context struct {
	context.Context
	Current         *quotes.Quote
	Prev            []*quotes.Quote // include current
	balance         float64
	balanceMutex    *sync.RWMutex
	holdingCast     float64
	holdingQuantity uint64
	holdingMutex    *sync.RWMutex
}

func NewContext(ctx context.Context, balance float64) *Context {
	return &Context{
		Context:         ctx,
		balance:         balance,
		balanceMutex:    new(sync.RWMutex),
		holdingCast:     0,
		holdingQuantity: 0,
		holdingMutex:    new(sync.RWMutex),
	}
}

func (c Context) Balance() float64 {
	c.balanceMutex.RLock()
	defer c.balanceMutex.RUnlock()

	return c.balance
}

func (c Context) Holding() (float64, uint64) {
	c.holdingMutex.RLock()
	defer c.holdingMutex.RUnlock()

	return c.holdingCast, c.holdingQuantity
}

func (c *Context) Buy(price float64, quantity uint64) (*TradeResult, error) {
	c.balanceMutex.Lock()
	defer c.balanceMutex.Unlock()

	err := c.buyValidate(price, quantity)
	if err != nil {
		return nil, err
	}

	c.holdingMutex.Lock()
	defer c.holdingMutex.Unlock()

	buyAmount := price * float64(quantity)
	holdingAmount := c.holdingCast * float64(c.holdingQuantity)
	tax := c.buyTax(price, quantity)

	c.balance -= buyAmount + tax
	c.holdingQuantity = c.holdingQuantity + quantity
	c.holdingCast = (holdingAmount + buyAmount) / float64(c.holdingQuantity)

	result := &TradeResult{
		Price:           price,
		Quantity:        quantity,
		HoldingCast:     c.holdingCast,
		HoldingQuantity: c.holdingQuantity,
	}

	// zap.L().Debug("buy successfully",
	// 	zap.Time("time", time.Unix(int64(c.Current.Timestamp), 0)),
	// 	zap.Float64("price", price),
	// 	zap.Uint64("quantity", quantity),
	// 	zap.Float64("banlance", c.balance),
	// 	zap.Float64("holdingCast", c.holdingCast),
	// 	zap.Uint64("holdingQuantity", c.holdingQuantity),
	// 	zap.Float64("tax", tax))

	return result, nil
}

func (c Context) buyValidate(price float64, quantity uint64) error {
	tax := c.buyTax(price, quantity)

	if price*float64(quantity)+tax > c.balance {
		zap.L().Warn("not enough balance",
			zap.Float64("price", price),
			zap.Uint64("quantity", quantity),
			zap.Float64("balance", c.balance))
		return ErrNotEnoughBalance
	}

	if price < float64(c.Current.Low) || price > float64(c.Current.High) {
		zap.L().Warn("price out of range",
			zap.Float64("price", price),
			zap.Float32("high", c.Current.High),
			zap.Float32("low", c.Current.Low))
		return ErrPriceOutOfRange
	}

	if quantity <= 0 || quantity > c.Current.Volume {
		zap.L().Warn("quantity out of range",
			zap.Uint64("quantity", quantity),
			zap.Uint64("volume", c.Current.Volume))
		return ErrQuantityOutOfRange
	}

	return nil
}

func (c Context) buyTax(price float64, quantity uint64) float64 {
	amount := price * float64(quantity)

	stampTax := float64(0)
	transferFee := amount * 0.0002
	if transferFee < 1 {
		transferFee = 1
	}

	commission := amount * 0.003
	if commission < 5 {
		commission = 5
	}

	return stampTax + float64(transferFee) + float64(commission)
}

func (c *Context) Sell(ctx context.Context, price float64, quantity uint64) (*TradeResult, error) {
	c.holdingMutex.Lock()
	defer c.holdingMutex.Unlock()

	err := c.sellValidate(price, quantity)
	if err != nil {
		return nil, err
	}

	c.balanceMutex.Lock()
	defer c.balanceMutex.Unlock()

	tax := c.sellTax(price, quantity)
	sellAmount := price * float64(quantity)
	c.balance += sellAmount - tax

	c.holdingQuantity -= quantity
	if c.holdingQuantity == 0 {
		c.holdingCast = 0
	}

	result := &TradeResult{
		Price:           price,
		Quantity:        quantity,
		HoldingCast:     c.holdingCast,
		HoldingQuantity: c.holdingQuantity,
	}

	// zap.L().Debug("sell successfully",
	// 	zap.Time("time", time.Unix(int64(c.Current.Timestamp), 0)),
	// 	zap.Float64("price", price),
	// 	zap.Uint64("quantity", quantity),
	// 	zap.Float64("banlance", c.balance),
	// 	zap.Float64("holdingCast", c.holdingCast),
	// 	zap.Uint64("holdingQuantity", c.holdingQuantity),
	// 	zap.Float64("profile", (price-c.holdingCast)*float64(quantity)-tax),
	// 	zap.Float64("tax", tax))

	return result, nil
}

func (c Context) sellValidate(price float64, quantity uint64) error {
	if quantity <= 0 || quantity > c.holdingQuantity {
		zap.L().Warn("quantity out of range",
			zap.Uint64("quantity", quantity),
			zap.Uint64("holdingQuantity", c.holdingQuantity))
		return ErrQuantityOutOfRange
	}

	if quantity > c.Current.Volume {
		zap.L().Warn("quantity out of range",
			zap.Uint64("quantity", quantity),
			zap.Uint64("volume", c.Current.Volume))
		return ErrQuantityOutOfRange
	}

	if price < float64(c.Current.Low) || price > float64(c.Current.High) {
		zap.L().Warn("price out of range",
			zap.Float64("price", price),
			zap.Float32("high", c.Current.High),
			zap.Float32("low", c.Current.Low))
		return ErrPriceOutOfRange
	}

	return nil
}

func (c Context) sellTax(price float64, quantity uint64) float64 {
	amount := price * float64(quantity)

	stampTax := amount * 0.001
	transferFee := amount * 0.0002
	if transferFee < 1 {
		transferFee = 1
	}

	commission := amount * 0.003
	if commission < 5 {
		commission = 5
	}

	return stampTax + float64(transferFee) + float64(commission)
}
