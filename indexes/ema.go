package indexes

import (
	"math"

	"github.com/nzai/qr/quotes"
)

type EMA struct {
	Timestamp uint64
	Value     float32
}

type EMAIndex struct {
	n     int
	round bool
}

func NewEMAIndex(n int, round bool) *EMAIndex {
	return &EMAIndex{n: n, round: round}
}

func (s EMAIndex) Calculate(qs []*quotes.Quote) ([]*EMA, error) {
	if len(qs) == 0 {
		return []*EMA{}, nil
	}

	var value float32
	emas := make([]*EMA, 0, len(qs))
	for index, q := range qs {
		if index == 0 {
			value = q.Close
		} else {
			value = (q.Close*2 + float32(s.n-1)*emas[index-1].Value) / float32(s.n+1)

			if s.round {
				// round 2
				value = float32(math.Round(float64(value)*100) / 100)
			}
		}

		emas = append(emas, &EMA{
			Timestamp: q.Timestamp,
			Value:     value,
		})
	}

	return emas, nil
}
