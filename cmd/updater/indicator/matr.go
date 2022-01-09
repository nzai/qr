package indicator

import (
	"math"

	"github.com/nzai/qr/quotes"
)

type MoveAverageTrueRange struct {
	peroid    int
	ma        *MoveAverage
	lastClose float64
}

func NewMoveAverageTrueRange(peroid, precision int) *MoveAverageTrueRange {
	return &MoveAverageTrueRange{
		peroid: peroid,
		ma:     NewMoveAverage(peroid, precision),
	}
}

func (ma MoveAverageTrueRange) Value() float64 {
	return ma.ma.Value()
}

func (ma MoveAverageTrueRange) Values() []float64 {
	return ma.ma.values
}

func (ma *MoveAverageTrueRange) Append(quote *quotes.Quote) float64 {
	if len(ma.ma.values) == 0 {
		ma.lastClose = float64(quote.Close)
		return ma.ma.Append(float64(quote.High - quote.Low))
	}

	tr := float64(quote.High - quote.Low)
	if math.Abs(float64(quote.High)-ma.lastClose) > tr {
		tr = math.Abs(float64(quote.High) - ma.lastClose)
	}

	if math.Abs(ma.lastClose-float64(quote.Low)) > tr {
		tr = math.Abs(ma.lastClose - float64(quote.Low))
	}

	ma.lastClose = float64(quote.Close)

	return ma.ma.Append(tr)
}
