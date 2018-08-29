package quotes

import (
	"fmt"
	"io"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// Exchange 交易所
type Exchange struct {
	Code string
}

// Encode 编码
func (e Exchange) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.String(e.Code)
	if err != nil {
		zap.L().Error("encode exchange code failed", zap.Error(err), zap.Any("exchange", e))
		return err
	}

	return nil
}

// Decode 解码
func (e *Exchange) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	code, err := br.String()
	if err != nil {
		zap.L().Error("decode exchange code failed", zap.Error(err))
		return err
	}

	e.Code = code

	return nil
}

// Equal 是否相同
func (e Exchange) Equal(s Exchange) error {

	if e.Code != e.Code {
		return fmt.Errorf("exchange code %s is different from %s", e.Code, s.Code)
	}

	return nil
}
