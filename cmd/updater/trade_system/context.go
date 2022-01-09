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

	c.balance -= buyAmount
	c.holdingQuantity = c.holdingQuantity + quantity
	c.holdingCast = (holdingAmount + buyAmount) / float64(c.holdingQuantity)

	result := &TradeResult{
		Price:           price,
		Quantity:        quantity,
		HoldingCast:     c.holdingCast,
		HoldingQuantity: c.holdingQuantity,
	}

	zap.L().Info("buy successfully",
		zap.Float64("price", price),
		zap.Uint64("quantity", quantity),
		zap.Float64("banlance", c.balance),
		zap.Float64("holdingCast", c.holdingCast),
		zap.Uint64("holdingQuantity", c.holdingQuantity))

	return result, nil
}

func (c Context) buyValidate(price float64, quantity uint64) error {
	if price*float64(quantity) > c.balance {
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

	if quantity > c.Current.Volume {
		zap.L().Warn("quantity out of range",
			zap.Uint64("quantity", quantity),
			zap.Uint64("volume", c.Current.Volume))
		return ErrQuantityOutOfRange
	}

	return nil
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

	sellAmount := price * float64(quantity)
	c.balance += sellAmount

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

	zap.L().Info("sell successfully",
		zap.Float64("price", price),
		zap.Uint64("quantity", quantity),
		zap.Float64("banlance", c.balance),
		zap.Float64("holdingCast", c.holdingCast),
		zap.Uint64("holdingQuantity", c.holdingQuantity))

	return result, nil
}

func (c Context) sellValidate(price float64, quantity uint64) error {
	if quantity > c.holdingQuantity {
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
