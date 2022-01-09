package indicator

import "math"

type MoveAverage struct {
	peroid    int
	precision int
	values    []float64
}

func NewMoveAverage(peroid, precision int) *MoveAverage {
	if peroid < 1 {
		panic("peroid out of range")
	}

	if precision < 0 {
		panic("precision out of range")
	}

	return &MoveAverage{
		peroid:    peroid,
		precision: precision,
		values:    make([]float64, 0),
	}
}

func (ma MoveAverage) Value() float64 {
	if len(ma.values) == 0 {
		return 0
	}

	return ma.values[len(ma.values)-1]
}

func (ma MoveAverage) Values() []float64 {
	return ma.values
}

func (ma *MoveAverage) Append(value float64) float64 {
	value = ma.floor(value)

	var newValue float64
	if len(ma.values) == 0 {
		newValue = value
	} else {
		newValue = ma.floor((ma.values[len(ma.values)-1]*float64(ma.peroid-1) + value) / float64(ma.peroid))
	}

	ma.values = append(ma.values, newValue)

	return newValue
}

func (ma MoveAverage) floor(value float64) float64 {
	times := math.Pow10(ma.precision)
	return math.Floor(value*times) / times
}
