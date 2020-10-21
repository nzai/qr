package indexes

import (
	"math"
	"testing"

	"github.com/nzai/qr/quotes"
)

func TestEMAIndex_Calculate(t *testing.T) {
	qs := []*quotes.Quote{
		{Timestamp: 1, Close: 97.04},
		{Timestamp: 2, Close: 92.64},
		{Timestamp: 3, Close: 92.41},
		{Timestamp: 4, Close: 94.61},
		{Timestamp: 5, Close: 93.05},
		{Timestamp: 6, Close: 94.84},
		{Timestamp: 7, Close: 95.99},
		{Timestamp: 8, Close: 106.06},
		{Timestamp: 9, Close: 108.73},
		{Timestamp: 10, Close: 109.46},
	}

	want := []*EMA{
		{Timestamp: 1, Value: 97.04},
		{Timestamp: 2, Value: 94.84},
		{Timestamp: 3, Value: 93.63},
		{Timestamp: 4, Value: 94.12},
		{Timestamp: 5, Value: 93.58},
		{Timestamp: 6, Value: 94.21},
		{Timestamp: 7, Value: 95.10},
		{Timestamp: 8, Value: 100.58},
		{Timestamp: 9, Value: 104.65},
		{Timestamp: 10, Value: 107.06},
	}

	emai := NewEMAIndex(3)
	emas, err := emai.Calculate(qs)
	if err != nil {
		t.Errorf("EMAIndex.Calculate() error = %v", err)
		return
	}

	if len(emas) != len(want) {
		t.Errorf("emas length mismatch, got %d, want %d", len(emas), len(want))
		return
	}

	for index, ema := range emas {
		if ema.Timestamp != want[index].Timestamp {
			t.Errorf("emas[%d] timestamp not equal, got %d, want %d", ema.Timestamp, ema.Timestamp, want[index].Timestamp)
		}

		if math.Abs(float64(ema.Value-want[index].Value)) >= 0.01 {
			t.Errorf("emas[%d] value not equal, got %f, want %f", ema.Timestamp, ema.Value, want[index].Value)
		}
	}
}
