package quotes

import (
	"fmt"
	"io"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// Company define company
type Company struct {
	Name string
	Code string // symbol
}

// Encode encode company to io.Writer
func (c Company) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.String(c.Code)
	if err != nil {
		zap.L().Error("encode company code failed", zap.Error(err), zap.Any("company", c))
		return err
	}

	_, err = bw.String(c.Name)
	if err != nil {
		zap.L().Error("encode company name failed", zap.Error(err), zap.Any("company", c))
		return err
	}

	return nil
}

// Decode decode company from io.Reader
func (c *Company) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	code, err := br.String()
	if err != nil {
		zap.L().Error("decode company code failed", zap.Error(err))
		return err
	}

	name, err := br.String()
	if err != nil {
		zap.L().Error("decode company name failed", zap.Error(err))
		return err
	}

	c.Code = code
	c.Name = name

	return nil
}

// Equal check company is equal
func (c Company) Equal(s Company) error {
	if c.Code != s.Code {
		return fmt.Errorf("company code %s is different from %s", c.Code, s.Code)
	}

	if c.Name != s.Name {
		return fmt.Errorf("company name %s is different from %s", c.Name, s.Name)
	}

	return nil
}

// CompanyMap define company map
type CompanyMap map[string]*Company

// Encode encode company map to io.Writer
func (m CompanyMap) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.Int(len(m))
	if err != nil {
		zap.L().Error("encode companies count failed", zap.Error(err), zap.Int("length", len(m)))
		return err
	}

	for _, company := range m {
		err = company.Encode(bw)
		if err != nil {
			zap.L().Error("encode company failed", zap.Error(err), zap.Any("company", company))
			return err
		}
	}

	return nil
}

// Decode decode company map from io.Reader
func (m *CompanyMap) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	count, err := br.Int()
	if err != nil {
		zap.L().Error("decode companies count failed", zap.Error(err))
		return err
	}

	*m = make(map[string]*Company, count)
	for index := 0; index < count; index++ {
		company := new(Company)
		err = company.Decode(br)
		if err != nil {
			zap.L().Error("decode company failed", zap.Error(err))
			return err
		}

		(*m)[company.Code] = company
	}

	return nil
}
