package quotes

import (
	"fmt"
	"io"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// Dividend define stock dividend
type Dividend struct {
	Enable    bool
	Timestamp uint64
	Amount    float32
}

// Encode encode dividend to io.Writer
func (d Dividend) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.Bool(d.Enable)
	if err != nil {
		zap.L().Error("encode dividend enable failed", zap.Error(err), zap.Bool("enable", d.Enable))
		return err
	}

	if !d.Enable {
		return nil
	}

	_, err = bw.UInt64(d.Timestamp)
	if err != nil {
		zap.L().Error("encode dividend timestamp failed", zap.Error(err), zap.Uint64("timestamp", d.Timestamp))
		return err
	}

	_, err = bw.Float32(d.Amount)
	if err != nil {
		zap.L().Error("encode dividend amount failed", zap.Error(err), zap.Float32("amount", d.Amount))
		return err
	}

	return nil
}

// Decode decode dividend from io.Reader
func (d *Dividend) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	enable, err := br.Bool()
	if err != nil {
		zap.L().Error("decode dividend enable failed", zap.Error(err))
		return err
	}

	if !enable {
		return nil
	}

	timestamp, err := br.UInt64()
	if err != nil {
		zap.L().Error("decode dividend timestamp failed", zap.Error(err))
		return err
	}

	amount, err := br.Float32()
	if err != nil {
		zap.L().Error("decode dividend amount failed", zap.Error(err))
		return err
	}

	d.Enable = enable
	d.Timestamp = timestamp
	d.Amount = amount

	return nil
}

// Equal check dividend is equal
func (d Dividend) Equal(s Dividend) error {

	if d.Enable != s.Enable {
		return fmt.Errorf("dividend enable %v is different from %v", d.Enable, s.Enable)
	}

	if d.Timestamp != s.Timestamp {
		return fmt.Errorf("dividend timestamp %d is different from %d", d.Timestamp, s.Timestamp)
	}

	if d.Amount != s.Amount {
		return fmt.Errorf("dividend amount %f is different from %f", d.Amount, s.Amount)
	}

	return nil
}
