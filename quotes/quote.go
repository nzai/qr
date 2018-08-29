package quotes

import (
	"fmt"
	"io"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// Quote 报价
type Quote struct {
	Timestamp uint64
	Open      float32
	Close     float32
	High      float32
	Low       float32
	Volume    uint64
}

// Encode 编码
func (q Quote) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.UInt64(q.Timestamp)
	if err != nil {
		zap.L().Error("encode quote timestamp failed", zap.Error(err), zap.Uint64("timestamp", q.Timestamp))
		return err
	}

	_, err = bw.Float32(q.Open)
	if err != nil {
		zap.L().Error("encode quote open failed", zap.Error(err), zap.Float32("open", q.Open))
		return err
	}

	_, err = bw.Float32(q.Close)
	if err != nil {
		zap.L().Error("encode quote close failed", zap.Error(err), zap.Float32("close", q.Close))
		return err
	}

	_, err = bw.Float32(q.High)
	if err != nil {
		zap.L().Error("encode quote open failed", zap.Error(err), zap.Float32("high", q.High))
		return err
	}

	_, err = bw.Float32(q.Low)
	if err != nil {
		zap.L().Error("encode quote open failed", zap.Error(err), zap.Float32("low", q.Low))
		return err
	}

	_, err = bw.UInt64(q.Volume)
	if err != nil {
		zap.L().Error("encode quote open failed", zap.Error(err), zap.Uint64("volume", q.Volume))
		return err
	}

	return nil
}

// Decode 解码
func (q *Quote) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	timestamp, err := br.UInt64()
	if err != nil {
		zap.L().Error("decode quote timestamp failed", zap.Error(err))
		return err
	}

	open, err := br.Float32()
	if err != nil {
		zap.L().Error("decode quote open failed", zap.Error(err))
		return err
	}

	_close, err := br.Float32()
	if err != nil {
		zap.L().Error("decode quote close failed", zap.Error(err))
		return err
	}

	high, err := br.Float32()
	if err != nil {
		zap.L().Error("decode quote high failed", zap.Error(err))
		return err
	}

	low, err := br.Float32()
	if err != nil {
		zap.L().Error("decode quote low failed", zap.Error(err))
		return err
	}

	volume, err := br.UInt64()
	if err != nil {
		zap.L().Error("decode quote volume failed", zap.Error(err))
		return err
	}

	q.Timestamp = timestamp
	q.Open = open
	q.Close = _close
	q.High = high
	q.Low = low
	q.Volume = volume

	return nil
}

// Equal 是否相同
func (q Quote) Equal(s Quote) error {

	if q.Timestamp != s.Timestamp {
		return fmt.Errorf("quote timestamp %d is different from %d", q.Timestamp, s.Timestamp)
	}

	if q.Open != s.Open {
		return fmt.Errorf("quote open %.2f is different from %.2f", q.Open, s.Open)
	}

	if q.Close != s.Close {
		return fmt.Errorf("quote close %.2f is different from %.2f", q.Close, s.Close)
	}

	if q.High != s.High {
		return fmt.Errorf("quote high %.2f is different from %.2f", q.High, s.High)
	}

	if q.Low != s.Low {
		return fmt.Errorf("quote low %.2f is different from %.2f", q.Low, s.Low)
	}

	if q.Volume != s.Volume {
		return fmt.Errorf("quote volume %d is different from %d", q.Volume, s.Volume)
	}

	return nil
}
