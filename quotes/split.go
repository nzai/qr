package quotes

import (
	"fmt"
	"io"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// Split define stock split
type Split struct {
	Enable      bool
	Timestamp   uint64  `json:"date"`
	Numerator   float32 `json:"numerator"`
	Denominator float32 `json:"denominator"`
}

// Encode encode split to io.Writer
func (s Split) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.Bool(s.Enable)
	if err != nil {
		zap.L().Error("encode split enable failed", zap.Error(err), zap.Bool("enable", s.Enable))
		return err
	}

	if !s.Enable {
		return nil
	}

	_, err = bw.UInt64(s.Timestamp)
	if err != nil {
		zap.L().Error("encode split timestamp failed", zap.Error(err), zap.Uint64("timestamp", s.Timestamp))
		return err
	}

	_, err = bw.Float32(s.Numerator)
	if err != nil {
		zap.L().Error("encode split numerator failed", zap.Error(err), zap.Float32("numerator", s.Numerator))
		return err
	}

	_, err = bw.Float32(s.Denominator)
	if err != nil {
		zap.L().Error("encode split denominator failed", zap.Error(err), zap.Float32("denominator", s.Denominator))
		return err
	}

	return nil
}

// Decode decode split from io.Reader
func (s *Split) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	enable, err := br.Bool()
	if err != nil {
		zap.L().Error("decode split enable failed", zap.Error(err))
		return err
	}

	if !enable {
		return nil
	}

	timestamp, err := br.UInt64()
	if err != nil {
		zap.L().Error("decode split timestamp failed", zap.Error(err))
		return err
	}

	numerator, err := br.Float32()
	if err != nil {
		zap.L().Error("decode split numerator failed", zap.Error(err))
		return err
	}

	denominator, err := br.Float32()
	if err != nil {
		zap.L().Error("decode split denominator failed", zap.Error(err))
		return err
	}

	s.Enable = enable
	s.Timestamp = timestamp
	s.Numerator = numerator
	s.Denominator = denominator

	return nil
}

// Equal check split is equal
func (s Split) Equal(t Split) error {

	if s.Enable != t.Enable {
		return fmt.Errorf("split enable %v is different from %v", s.Enable, t.Enable)
	}

	if s.Timestamp != t.Timestamp {
		return fmt.Errorf("split timestamp %d is different from %d", s.Timestamp, t.Timestamp)
	}

	if s.Numerator != t.Numerator {
		return fmt.Errorf("split numerator %f is different from %f", s.Numerator, t.Numerator)
	}

	if s.Denominator != t.Denominator {
		return fmt.Errorf("split denominator %f is different from %f", s.Denominator, t.Denominator)
	}

	return nil
}
