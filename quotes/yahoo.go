package quotes

import (
	"errors"

	"go.uber.org/zap"
)

var (
	// YahooNotFoundCode define errors raised by yahoo finace on code not found
	YahooNotFoundCode = "Not Found"
	// ErrYahooSymbolNotFound define errors raised by yahoo finace on symblo not found
	ErrYahooSymbolNotFound = errors.New("symbol not foud")
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
				TradingPeriods struct {
					Pres     [][]YahooPeroid `json:"pre"`
					Regulars [][]YahooPeroid `json:"regular"`
					Posts    [][]YahooPeroid `json:"post"`
				} `json:"tradingPeriods"`
				DataGranularity string   `json:"dataGranularity"`
				ValidRanges     []string `json:"validRanges"`
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
func (q YahooQuote) ToCompanyDailyQuote(company *Company, start, end uint64) *DailyQuote {
	dq := &DailyQuote{
		Company: company,
		Pre:     new(Serial),
		Regular: new(Serial),
		Post:    new(Serial),
	}

	tp := q.Chart.Result[0].Meta.TradingPeriods
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
		if ts >= tp.Pres[0][0].Start && ts < tp.Pres[0][0].End {
			*dq.Pre = append(*dq.Pre, quote)
		} else if ts >= tp.Regulars[0][0].Start && ts < tp.Regulars[0][0].End {
			*dq.Regular = append(*dq.Regular, quote)
		} else if ts >= tp.Posts[0][0].Start && ts < tp.Posts[0][0].End {
			*dq.Post = append(*dq.Post, quote)
		} else {
			continue
		}
	}

	return dq
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
