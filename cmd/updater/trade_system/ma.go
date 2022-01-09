package trade_system

import (
	"context"

	"github.com/nzai/qr/cmd/updater/indicator"
)

type MoveAverage struct {
	indicate *indicator.MoveAverage
	call     bool
}

func NewMA(peroid, precision int) *MoveAverage {
	return &MoveAverage{
		indicate: indicator.NewMoveAverage(peroid, precision),
	}
}

func (a *MoveAverage) Init(ctx context.Context) error {
	return nil
}

func (a *MoveAverage) Next(ctx *Context) error {
	ma := a.indicate.Value()
	defer a.indicate.Append(float64(ctx.Current.Close))

	if ma == 0 {
		return nil
	}

	var err error
	if !a.call && float64(ctx.Current.Close) > ma {
		quantity := uint64(ctx.Balance() * 0.995 / float64(ctx.Current.Close))
		if quantity <= 0 {
			return nil
		}
		_, err = ctx.Buy(float64(ctx.Current.Close), quantity)
		if err != nil {
			return err
		}

		a.call = true
		return nil
	}

	if a.call && float64(ctx.Current.Close) < ma {
		_, quantity := ctx.Holding()
		if quantity <= 0 {
			return nil
		}

		_, err = ctx.Sell(ctx, float64(ctx.Current.Close), quantity)
		if err != nil {
			return err
		}

		a.call = false
		return nil
	}

	return nil
}

func (a MoveAverage) Close(ctx context.Context) error {
	return nil
}
