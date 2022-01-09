package indicator

import "testing"

func TestMoveAverage_Append(t *testing.T) {
	ma := NewMoveAverage(5, 2)

	cases := []struct {
		input float64
		want  float64
	}{
		{
			input: 1.0,
			want:  1.0,
		},
		{
			input: 1.1,
			want:  1.02,
		},
		{
			input: 1.2,
			want:  1.05,
		},
		{
			input: 1.1,
			want:  1.06,
		},
		{
			input: 1.6,
			want:  1.16,
		},
	}

	var got float64
	for _, _case := range cases {
		got = ma.Append(_case.input)
		if got != _case.want {
			t.Errorf("MoveAverage.Append() = %v, want %v", got, _case.want)
		}
	}
}
