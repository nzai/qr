package trade_system

import (
	"context"

	"github.com/nzai/qr/cmd/updater/indicator"
)

type Turtle struct {
	matr      *indicator.MoveAverageTrueRange
	maIn      *indicator.MoveAverage
	maOut     *indicator.MoveAverage
	percent   float64
	call      bool
	n         float64
	nextPrice float64
	stopPrice float64
}

func NewTurtle(peroid, precision int, percent float64) *Turtle {
	return &Turtle{
		matr:    indicator.NewMoveAverageTrueRange(peroid, precision),
		maIn:    indicator.NewMoveAverage(peroid, precision),
		maOut:   indicator.NewMoveAverage(peroid/2, precision),
		percent: percent,
	}
}

func (t *Turtle) Init(ctx context.Context) error {
	return nil
}

func (t *Turtle) Next(ctx *Context) error {
	defer func() {
		t.matr.Append(ctx.Current)
		t.maIn.Append(float64(ctx.Current.Close))
		t.maOut.Append(float64(ctx.Current.Close))
	}()

	if len(t.maIn.Values()) == 0 {
		return nil
	}

	maIn := t.maIn.Value()
	n := t.matr.Value()

	var err error
	if !t.call {
		_, holdingQuantity := ctx.Holding()
		if holdingQuantity == 0 && float64(ctx.Current.Close) > maIn {
			// 入市
			quantity := uint64(ctx.Balance() * t.percent / (n * float64(ctx.Current.Close)))
			if quantity == 0 {
				return nil
			}

			_, err = ctx.Buy(float64(ctx.Current.Close), quantity)
			if err != nil {
				return err
			}

			t.call = true
			t.n = n
			t.nextPrice = float64(ctx.Current.Close) + n*0.5
			t.stopPrice = t.nextPrice - n*2

			return nil
		}

		if holdingQuantity > 0 && float64(ctx.Current.Close) > t.nextPrice {
			// 加仓
			for price := t.nextPrice; price < float64(ctx.Current.Close); price += n * 0.5 {
				quantity := uint64(ctx.Balance() * t.percent / (n * price))
				if quantity == 0 {
					return nil
				}

				_, err = ctx.Buy(price, quantity)
				if err != nil {
					return err
				}

				t.nextPrice += n * 0.5
				t.stopPrice = t.nextPrice - n*2
			}

			return nil
		}
	}

	maOut := t.maOut.Value()
	if t.call && (float64(ctx.Current.Close) < maOut || float64(ctx.Current.Close) < t.stopPrice) {
		_, quantity := ctx.Holding()
		if quantity <= 0 {
			return nil
		}

		_, err = ctx.Sell(ctx, float64(ctx.Current.Close), quantity)
		if err != nil {
			return err
		}

		t.call = false
		t.n = 0
		t.nextPrice = 0
		t.stopPrice = 0

		return nil
	}

	return nil
}

func (t Turtle) Close(ctx context.Context) error {
	return nil
}
