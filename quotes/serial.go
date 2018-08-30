package quotes

import (
	"fmt"
	"io"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// SerialType define quotes type
type SerialType int

const (
	// SerialTypePre before trading peroid
	SerialTypePre SerialType = iota + 1
	// SerialTypeRegular in the trading peroid
	SerialTypeRegular
	// SerialTypePost after trading peroid
	SerialTypePost
)

func (t SerialType) String() string {
	switch t {
	case SerialTypePre:
		return "Pre"
	case SerialTypeRegular:
		return "Regular"
	case SerialTypePost:
		return "Post"
	default:
		return fmt.Sprintf("unknown quote serial type: %d", t)
	}
}

// Serial define quotes
type Serial []Quote

// Encode encode quotes to io.Writer
func (s Serial) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.Int(len(s))
	if err != nil {
		zap.L().Error("encode quote serial length failed", zap.Error(err), zap.Int("length", len(s)))
		return err
	}

	for _, quote := range s {
		err = quote.Encode(w)
		if err != nil {
			zap.L().Error("encode quote failed", zap.Error(err), zap.Any("quote", quote))
			return err
		}
	}

	return nil
}

// Decode decode quotes from io.Reader
func (s *Serial) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	count, err := br.Int()
	if err != nil {
		zap.L().Error("decode quote serial length failed", zap.Error(err))
		return err
	}

	*s = make([]Quote, count)
	for index := 0; index < count; index++ {

		err = (*s)[index].Decode(r)
		if err != nil {
			zap.L().Error("decode quote failed", zap.Error(err))
			return err
		}
	}

	return nil
}

// Equal check quotes is equal
func (s Serial) Equal(q Serial) error {

	if len(s) != len(q) {
		return fmt.Errorf("quote serial length %d is different from %d", len(s), len(q))
	}

	for index, quote := range s {
		err := quote.Equal(q[index])
		if err != nil {
			zap.L().Error("quote is not equal", zap.Any("from", quote), zap.Any("to", q[index]))
			return err
		}
	}

	return nil
}

func (s Serial) Len() int {
	return len(s)
}
func (s Serial) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s Serial) Less(i, j int) bool {
	return s[i].Timestamp < s[j].Timestamp
}

// Rollup rollup quotes to a summary
func (s Serial) Rollup() *Quote {

	if len(s) == 0 {
		return &Quote{}
	}

	quote := &Quote{
		Timestamp: s[0].Timestamp,
		Open:      s[0].Open,
		Close:     s[len(s)-1].Close,
		High:      s[0].High,
		Low:       s[0].Low,
		Volume:    s[0].Volume,
	}

	for index := 1; index < len(s); index++ {
		if s[index].High > quote.High {
			quote.High = s[index].High
		}

		if s[index].Low < quote.Low {
			quote.Low = s[index].Low
		}

		quote.Volume += s[index].Volume
	}

	return quote
}
