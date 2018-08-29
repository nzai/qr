package quotes

import (
	"fmt"
	"io"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// Company 公司
type Company struct {
	Name string // 名称
	Code string // 代码
}

// Encode 编码
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

// Decode 解码
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

// Equal 是否相同
func (c Company) Equal(s Company) error {

	if c.Code != s.Code {
		return fmt.Errorf("company code %s is different from %s", c.Code, s.Code)
	}

	if c.Name != s.Name {
		return fmt.Errorf("company name %s is different from %s", c.Name, s.Name)
	}

	return nil
}

// CompanyList 公司列表
type CompanyList []*Company

func (l CompanyList) Len() int {
	return len(l)
}
func (l CompanyList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l CompanyList) Less(i, j int) bool {
	return l[i].Code < l[j].Code
}

// Encode 编码
func (l CompanyList) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.Int(len(l))
	if err != nil {
		zap.L().Error("encode companies count failed", zap.Error(err), zap.Int("length", len(l)))
		return err
	}

	for _, company := range l {
		err = company.Encode(bw)
		if err != nil {
			zap.L().Error("encode company failed", zap.Error(err), zap.Any("company", company))
			return err
		}
	}

	return nil
}

// Decode 解码
func (l *CompanyList) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	count, err := br.Int()
	if err != nil {
		zap.L().Error("decode companies count failed", zap.Error(err))
		return err
	}

	*l = make([]*Company, count)
	for index := 0; index < count; index++ {
		(*l)[index] = new(Company)
		err = (*l)[index].Decode(br)
		if err != nil {
			zap.L().Error("decode company failed", zap.Error(err))
			return err
		}
	}

	return nil
}
