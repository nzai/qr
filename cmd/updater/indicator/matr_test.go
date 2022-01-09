package indicator

import (
	"testing"

	"github.com/nzai/qr/quotes"
)

func TestMoveAverageTrueRange_Append(t *testing.T) {
	matr := NewMoveAverageTrueRange(20, 4)
	cases := []struct {
		input *quotes.Quote
		want  float64
	}{
		{
			input: &quotes.Quote{High: 0.7220, Low: 0.7124, Close: 0.7124},
			want:  0.0095,
		},
		{
			input: &quotes.Quote{High: 0.7170, Low: 0.7073, Close: 0.7073},
			want:  0.0095,
		},
	}

	var got float64
	for _, _case := range cases {
		got = matr.Append(_case.input)
		if got != _case.want {
			t.Errorf("MoveAverageTrueRange.Append() = %v, want %v", got, _case.want)
		}
	}
}
