package trade_system

import "context"

type LongHold struct {
	first bool
}

func NewLongHold() *LongHold {
	return &LongHold{}
}

func (a *LongHold) Init(ctx context.Context) error {
	a.first = true
	return nil
}

func (a *LongHold) Next(ctx *Context) error {
	if !a.first {
		return nil
	}

	a.first = false
	quantity := uint64(ctx.Balance() / float64(ctx.Current.Close))
	if quantity <= 0 {
		return nil
	}

	_, err := ctx.Buy(float64(ctx.Current.Close), quantity)
	if err != nil {
		return err
	}

	return nil
}

func (a LongHold) Close(ctx context.Context) error {
	return nil
}
