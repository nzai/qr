package quotes

import (
	"encoding/json"
	"errors"

	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

var (
	// YahooNotFoundCode define errors raised by yahoo finace on code not found
	YahooNotFoundCode = "Not Found"
	// ErrYahooSymbolNotFound define errors raised by yahoo finace on symblo not found
	ErrYahooSymbolNotFound = errors.New("symbol not foud")
	// ErrYahooInvalidTradingPeroid define errors raised by yahoo invalid trading peroid
	ErrYahooInvalidTradingPeroid = errors.New("invalid trading peroid")
)

// YahooQuote define yahoo finace response structure
type YahooQuote struct {
	Chart struct {
		Result []struct {
			Meta struct {
				Currency             string  `json:"currency"`
				Symbol               string  `json:"symbol"`
				ExchangeName         string  `json:"exchangeName"`
				InstrumentType       string  `json:"instrumentType"`
				FirstTradeDate       int64   `json:"firstTradeDate"`
				GMTOffset            int64   `json:"gmtoffset"`
				Timezone             string  `json:"timezone"`
				PreviousClose        float32 `json:"previousClose"`
				Scale                int     `json:"scale"`
				CurrentTradingPeriod struct {
					Pre     YahooPeroid `json:"pre"`
					Regular YahooPeroid `json:"regular"`
					Post    YahooPeroid `json:"post"`
				} `json:"currentTradingPeriod"`
				TradingPeriods  json.RawMessage `json:"tradingPeriods"`
				DataGranularity string          `json:"dataGranularity"`
				ValidRanges     []string        `json:"validRanges"`
			} `json:"meta"`
			Timestamp []uint64 `json:"timestamp"`
			Events    struct {
				Dividends map[uint64]YahooDividend `json:"dividends"`
				Splits    map[uint64]YahooSplits   `json:"splits"`
			} `json:"events"`
			Indicators struct {
				Quotes []struct {
					Open   []float32 `json:"open"`
					Close  []float32 `json:"close"`
					High   []float32 `json:"high"`
					Low    []float32 `json:"low"`
					Volume []uint64  `json:"volume"`
				} `json:"quote"`
			} `json:"indicators"`
		} `json:"result"`
		Err *struct {
			Code        string `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	} `json:"chart"`
}

// Validate validate response is valid
func (q YahooQuote) Validate() error {
	// yahoo error
	if q.Chart.Err != nil {
		if q.Chart.Err.Code == YahooNotFoundCode {
			return ErrYahooSymbolNotFound
		}
		return errors.New(q.Chart.Err.Description)
	}

	if q.Chart.Result == nil || len(q.Chart.Result) == 0 {
		return errors.New("quote.Chart.Result is null")
	}

	if q.Chart.Result[0].Indicators.Quotes == nil || len(q.Chart.Result[0].Indicators.Quotes) == 0 {
		return errors.New("quote.Chart.Result[0].Indicators.Quotes is null")
	}

	// if len(q.Chart.Result[0].Meta.TradingPeriods) == 0 || len(q.Chart.Result[0].Meta.TradingPeriods[0]) == 0 {
	// 	return errors.New("quote.Chart.Result[0].Meta.TradingPeriods invalie")
	// }

	if len(q.Chart.Result[0].Events.Dividends) > 1 {
		zap.L().Warn("dividends count > 1",
			zap.Int("count", len(q.Chart.Result[0].Events.Dividends)),
			zap.Any("dividends", q.Chart.Result[0].Events.Dividends))
	}

	if len(q.Chart.Result[0].Events.Splits) > 1 {
		zap.L().Warn("splits count > 1",
			zap.Int("count", len(q.Chart.Result[0].Events.Splits)),
			zap.Any("splits", q.Chart.Result[0].Events.Splits))
	}

	result, _quote := q.Chart.Result[0], q.Chart.Result[0].Indicators.Quotes[0]

	// quotes count mismatch
	if len(result.Timestamp) != len(_quote.Open) ||
		len(result.Timestamp) != len(_quote.Close) ||
		len(result.Timestamp) != len(_quote.High) ||
		len(result.Timestamp) != len(_quote.Low) ||
		len(result.Timestamp) != len(_quote.Volume) {
		return errors.New("quotes count dismatch")
	}

	return nil
}

// ToCompanyDailyQuote convert yahoo finance response to company daily quote
func (q YahooQuote) ToCompanyDailyQuote(company *Company, start, end uint64) *CompanyDailyQuote {
	cdq := &CompanyDailyQuote{
		Company:  company,
		Dividend: &Dividend{Enable: false, Timestamp: 0, Amount: 0},
		Split:    &Split{Enable: false, Timestamp: 0, Numerator: 0, Denominator: 0},
		Pre:      new(Serial),
		Regular:  new(Serial),
		Post:     new(Serial),
	}

	for _, dividend := range q.Chart.Result[0].Events.Dividends {
		if dividend.Date < start || dividend.Date >= end {
			continue
		}

		cdq.Dividend.Enable = true
		cdq.Dividend.Timestamp = dividend.Date
		cdq.Dividend.Amount = dividend.Amount
		break
	}

	for _, split := range q.Chart.Result[0].Events.Splits {
		if split.Date < start || split.Date >= end {
			continue
		}

		cdq.Split.Enable = true
		cdq.Split.Timestamp = split.Date
		cdq.Split.Numerator = float32(split.Numerator)
		cdq.Split.Denominator = float32(split.Denominator)
		break
	}

	regularPeroid := q.getRegularTradingPeroid()
	if regularPeroid == nil {
		total := 0
		for _, ts := range q.Chart.Result[0].Timestamp {
			if ts < start || ts >= end {
				continue
			}

			total++
			if total < 5 {
				continue
			}

			zap.L().Error("invalid yahoo quote",
				zap.Uint64("start", start),
				zap.Uint64("end", end),
				zap.Any("quote", q))

			utils.GetWeChatService().SendMessage("get invalid trading peroid")
			break
		}

		return cdq
	}

	// tp := q.Chart.Result[0].Meta.TradingPeriods[0][0]
	qs := q.Chart.Result[0].Indicators.Quotes[0]
	for index, ts := range q.Chart.Result[0].Timestamp {
		// ignore all zero quote
		if qs.Open[index] == 0 && qs.Close[index] == 0 && qs.High[index] == 0 && qs.Low[index] == 0 && qs.Volume[index] == 0 {
			continue
		}

		quote := Quote{
			Timestamp: uint64(ts),
			Open:      qs.Open[index],
			Close:     qs.Close[index],
			High:      qs.High[index],
			Low:       qs.Low[index],
			Volume:    uint64(qs.Volume[index]),
		}

		//	Pre, Regular, Post
		if ts < regularPeroid.Start {
			*cdq.Pre = append(*cdq.Pre, quote)
		} else if ts >= regularPeroid.Start && ts < regularPeroid.End {
			*cdq.Regular = append(*cdq.Regular, quote)
		} else if ts >= regularPeroid.End {
			*cdq.Post = append(*cdq.Post, quote)
		}
	}

	return cdq
}

// getRegularTradingPeroid get regular trading peroid by uncertain structure
func (q YahooQuote) getRegularTradingPeroid() *YahooPeroid {
	tp1 := new(TradingPeroid1)
	err := json.Unmarshal(q.Chart.Result[0].Meta.TradingPeriods, tp1)
	if err == nil {
		if len(*tp1) > 0 && len((*tp1)[0]) > 0 {
			return &(*tp1)[0][0]
		}
	}

	tp2 := new(TradingPeroid2)
	err = json.Unmarshal(q.Chart.Result[0].Meta.TradingPeriods, tp2)
	if err == nil {
		if len(tp2.Regular) > 0 && len(tp2.Regular[0]) > 0 {
			return &tp2.Regular[0][0]
		}
	}

	return nil
}

// YahooPeroid define trading peroid
type YahooPeroid struct {
	Timezone  string `json:"timezone"`
	Start     uint64 `json:"start"`
	End       uint64 `json:"end"`
	GMTOffset int64  `json:"gmtoffset"`
}

// YahooDividend define stock dividend
type YahooDividend struct {
	Amount float32 `json:"amount"`
	Date   uint64  `json:"date"`
}

// YahooSplits define stock split
type YahooSplits struct {
	Date        uint64 `json:"date"`
	Numerator   uint32 `json:"numerator"`
	Denominator uint32 `json:"denominator"`
	Ratio       string `json:"splitRatio"`
}

// TradingPeroid1 define trading peroid
type TradingPeroid1 [][]YahooPeroid

// TradingPeroid2 define trading peroid
type TradingPeroid2 struct {
	Pre     [][]YahooPeroid `json:"pre"`
	Regular [][]YahooPeroid `json:"regular"`
	Post    [][]YahooPeroid `json:"post"`
}
