package indicator

import "math"

func Floor(value float64, precision int) float64 {
	times := math.Pow10(precision)
	return math.Floor(value*times) / times
}

func Round(value float64, precision int) float64 {
	times := math.Pow10(precision)
	return math.Round(value*times) / times
}
