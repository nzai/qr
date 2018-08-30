package quotes

import (
	"fmt"
	"io"
	"time"

	"github.com/nzai/bio"
	"go.uber.org/zap"
)

// ExchangeDailyQuote define exchange daily quote
type ExchangeDailyQuote struct {
	Version   uint8
	Exchange  string
	Date      time.Time
	Companies map[string]*Company
	Quotes    map[string]*DailyQuote
}

// Encode encode exchange daily quote to io.Writer
func (q ExchangeDailyQuote) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	_, err := bw.UInt8(q.Version)
	if err != nil {
		zap.L().Error("encode version failed", zap.Error(err), zap.Uint8("version", q.Version))
		return err
	}

	_, err = bw.String(q.Exchange)
	if err != nil {
		zap.L().Error("encode exchange code failed", zap.Error(err), zap.String("exchange", q.Exchange))
		return err
	}

	_, err = bw.Time(q.Date)
	if err != nil {
		zap.L().Error("encode date failed", zap.Error(err), zap.Time("date", q.Date))
		return err
	}

	_, err = bw.Int(len(q.Companies))
	if err != nil {
		zap.L().Error("encode company count failed", zap.Error(err), zap.Int("count", len(q.Companies)))
		return err
	}

	for _, company := range q.Companies {
		err = company.Encode(bw)
		if err != nil {
			zap.L().Error("encode companye failed", zap.Error(err), zap.Any("company", company))
			return err
		}
	}

	for companyCode, dailyQuote := range q.Quotes {
		err = dailyQuote.Encode(bw)
		if err != nil {
			zap.L().Error("encode daily quote failed", zap.Error(err), zap.Any("company", companyCode))
			return err
		}
	}

	return nil
}

// Decode decode exchange daily quote from io.Reader
func (q *ExchangeDailyQuote) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	version, err := br.UInt8()
	if err != nil {
		zap.L().Error("decode version failed", zap.Error(err))
		return err
	}

	exchange, err := br.String()
	if err != nil {
		zap.L().Error("decode exchange failed", zap.Error(err))
		return err
	}

	date, err := br.Time()
	if err != nil {
		zap.L().Error("decode date failed", zap.Error(err))
		return err
	}

	count, err := br.Int()
	if err != nil {
		zap.L().Error("decode company count failed", zap.Error(err))
		return err
	}

	companies := make(map[string]*Company, count)
	for index := 0; index < count; index++ {
		company := new(Company)
		err = company.Decode(br)
		if err != nil {
			zap.L().Error("decode company failed", zap.Error(err))
			return err
		}

		companies[company.Code] = company
	}

	q.Version = version
	q.Exchange = exchange
	q.Date = date
	q.Companies = companies

	return nil
}

// Equal check exchange daily quote is equal
func (q ExchangeDailyQuote) Equal(s ExchangeDailyQuote) error {

	if q.Version != s.Version {
		return fmt.Errorf("version %d is not equal from %d", q.Version, s.Version)
	}

	if q.Exchange != s.Exchange {
		return fmt.Errorf("exchange %s is not equal from %s", q.Exchange, s.Exchange)
	}

	if !q.Date.Equal(s.Date) {
		return fmt.Errorf("date %s is not equal from %s", q.Date.Format("2006-01-02"), s.Date.Format("2006-01-02"))
	}

	if len(q.Companies) != len(s.Companies) {
		return fmt.Errorf("companis count %d is not equal from %d", len(q.Companies), len(s.Companies))
	}

	for companyCode, company := range q.Companies {
		another, found := s.Companies[companyCode]
		if !found {
			return fmt.Errorf("company %s/%s is not found from another", companyCode, company.Name)
		}

		err := company.Equal(*another)
		if err != nil {
			return fmt.Errorf("company is not equal due to %v", err)
		}
	}

	if len(q.Quotes) != len(s.Quotes) {
		return fmt.Errorf("quotes count %d is not equal from %d", len(q.Quotes), len(s.Quotes))
	}

	for companyCode, dailyQuote := range q.Quotes {
		another, found := s.Quotes[companyCode]
		if !found {
			return fmt.Errorf("quote %s is not found from another", companyCode)
		}

		err := dailyQuote.Equal(*another)
		if err != nil {
			return fmt.Errorf("quote is not equal due to %v", err)
		}
	}

	return nil
}

// DailyQuote define company daily quote
type DailyQuote struct {
	Pre     *Serial
	Regular *Serial
	Post    *Serial
}

// Encode encode company daily quote to io.Writer
func (q DailyQuote) Encode(w io.Writer) error {
	bw := bio.NewBinaryWriter(w)

	err := q.Pre.Encode(bw)
	if err != nil {
		zap.L().Error("encode pre serial failed", zap.Error(err), zap.Int("count", len(*q.Pre)))
		return err
	}

	err = q.Regular.Encode(bw)
	if err != nil {
		zap.L().Error("encode regular serial failed", zap.Error(err), zap.Int("count", len(*q.Regular)))
		return err
	}

	err = q.Post.Encode(bw)
	if err != nil {
		zap.L().Error("encode post serial failed", zap.Error(err), zap.Int("count", len(*q.Post)))
		return err
	}

	return nil
}

// Decode decode company daily quote from io.Reader
func (q *DailyQuote) Decode(r io.Reader) error {
	br := bio.NewBinaryReader(r)

	pre := new(Serial)
	err := pre.Decode(br)
	if err != nil {
		zap.L().Error("decode pre serial failed", zap.Error(err))
		return err
	}

	regular := new(Serial)
	err = regular.Decode(br)
	if err != nil {
		zap.L().Error("decode regular serial failed", zap.Error(err))
		return err
	}

	post := new(Serial)
	err = post.Decode(br)
	if err != nil {
		zap.L().Error("decode post serial failed", zap.Error(err))
		return err
	}

	q.Pre = pre
	q.Regular = regular
	q.Post = post

	return nil
}

// Equal check company daily quote is equal
func (q DailyQuote) Equal(s DailyQuote) error {

	err := q.Pre.Equal(*s.Pre)
	if err != nil {
		return fmt.Errorf("pre serial is not equal due to %v", err)
	}

	err = q.Regular.Equal(*s.Regular)
	if err != nil {
		return fmt.Errorf("regular serial is not equal due to %v", err)
	}

	err = q.Post.Equal(*s.Post)
	if err != nil {
		return fmt.Errorf("post serial is not equal due to %v", err)
	}

	return nil
}
