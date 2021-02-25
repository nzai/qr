package model

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/nzai/netop"
	"github.com/nzai/qr/cmd/chart/entity"
	"go.uber.org/zap"
)

type YahooQuoteDownloader struct{}

var (
	_yahooQuoteDownloaderOnce sync.Once
	_yahooQuoteDownloader     *YahooQuoteDownloader
)

func GetYahooQuoteDownloader() *YahooQuoteDownloader {
	_yahooQuoteDownloaderOnce.Do(func() {
		_yahooQuoteDownloader = &YahooQuoteDownloader{}
	})

	return _yahooQuoteDownloader
}

func (s YahooQuoteDownloader) DailyAll(code string) (*entity.Quotes, error) {
	var responses []*YahooChartResponse
	year := time.Now().Year()
	for {
		zap.L().Debug("try to download", zap.Int("year", year))
		response, exists, err := s.DailyOfYear(code, year)
		if err != nil {
			return nil, err
		}

		if !exists {
			zap.L().Debug("download stop", zap.Int("year", year))
			break
		}

		responses = append(responses, response)
		zap.L().Debug("download success", zap.Int("year", year))

		year--
	}

	return s.responsesToQuote(responses), nil
}

func (s YahooQuoteDownloader) DailyOfYear(code string, year int) (*YahooChartResponse, bool, error) {
	start := time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
	end := start.AddDate(1, 0, 0)
	url := fmt.Sprintf("https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d&events=div|split|earn&lang=en-US&region=US&corsDomain=finance.yahoo.com",
		code, start.Unix(), end.Unix())

	response, err := netop.Get(url, netop.Retry(3, time.Second))
	if err != nil {
		return nil, false, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, false, nil
	}

	buffer, err := ioutil.ReadAll(response.Body)
	if err != nil {
		zap.L().Error("read response body failed", zap.Error(err))
		return nil, false, err
	}

	yr := new(YahooChartResponse)
	err = json.Unmarshal(buffer, yr)
	if err != nil {
		zap.L().Error("unmarshal json error", zap.Error(err), zap.ByteString("buffer", buffer))
		return nil, false, err
	}

	if !yr.Valid() {
		zap.L().Debug("response json invalid", zap.Int("year", year), zap.ByteString("buffer", buffer))
		return nil, false, nil
	}

	return yr, true, nil
}

func (s YahooQuoteDownloader) responsesToQuote(responses []*YahooChartResponse) *entity.Quotes {
	// max trade days of a year
	count := len(responses) * 365

	quote := &entity.Quotes{
		Timestamp: make([]int64, 0, count),
		Open:      make([]float64, 0, count),
		Close:     make([]float64, 0, count),
		High:      make([]float64, 0, count),
		Low:       make([]float64, 0, count),
		Volume:    make([]float64, 0, count),
	}

	for index := len(responses) - 1; index >= 0; index-- {
		q := responses[index].ToAdjQuotes()

		quote.Timestamp = append(quote.Timestamp, q.Timestamp...)
		quote.Open = append(quote.Open, q.Open...)
		quote.Close = append(quote.Close, q.Close...)
		quote.High = append(quote.High, q.High...)
		quote.Low = append(quote.Low, q.Low...)
		quote.Volume = append(quote.Volume, q.Volume...)
	}

	return quote
}

type YahooChartResponse struct {
	Chart struct {
		Result []struct {
			Meta struct {
				Currency        string          `json:"currency"`
				Symbol          string          `json:"symbol"`
				ExchangeName    string          `json:"exchangeName"`
				InstrumentType  string          `json:"instrumentType"`
				FirstTradeDate  int64           `json:"firstTradeDate"`
				GMTOffset       int64           `json:"gmtoffset"`
				Timezone        string          `json:"timezone"`
				PreviousClose   float32         `json:"previousClose"`
				Scale           int             `json:"scale"`
				TradingPeriods  json.RawMessage `json:"tradingPeriods"`
				DataGranularity string          `json:"dataGranularity"`
				ValidRanges     []string        `json:"validRanges"`
			} `json:"meta"`
			Timestamp  []int64 `json:"timestamp"`
			Indicators struct {
				Quote []struct {
					Open   []float64 `json:"open"`
					Close  []float64 `json:"close"`
					High   []float64 `json:"high"`
					Low    []float64 `json:"low"`
					Volume []int64   `json:"volume"`
				} `json:"quote"`
				Adjclose []struct {
					Adjclose []float64 `json:"adjclose"`
				} `json:"adjclose"`
			} `json:"indicators"`
		} `json:"result"`
		Err *struct {
			Code        string `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	} `json:"chart"`
}

func (r YahooChartResponse) Valid() bool {
	if len(r.Chart.Result) == 0 {
		zap.L().Debug("r.Chart.Result invalid")
		return false
	}

	if len(r.Chart.Result[0].Timestamp) == 0 {
		return true
	}

	if len(r.Chart.Result[0].Indicators.Quote) == 0 {
		zap.L().Debug("r.Chart.Result[0].Indicators.Quote invalid")
		return false
	}

	if len(r.Chart.Result[0].Indicators.Adjclose) == 0 {
		zap.L().Debug("r.Chart.Result[0].Indicators.Adjclose invalid")
		return false
	}

	count := len(r.Chart.Result[0].Timestamp)
	if len(r.Chart.Result[0].Indicators.Quote[0].Open) != count ||
		len(r.Chart.Result[0].Indicators.Quote[0].Close) != count ||
		len(r.Chart.Result[0].Indicators.Quote[0].High) != count ||
		len(r.Chart.Result[0].Indicators.Quote[0].Low) != count ||
		len(r.Chart.Result[0].Indicators.Quote[0].Volume) != count ||
		len(r.Chart.Result[0].Indicators.Adjclose[0].Adjclose) != count {
		zap.L().Debug("serial count invalid")
		return false
	}

	return true
}

func (r YahooChartResponse) ToAdjQuotes() *entity.Quotes {
	count := len(r.Chart.Result[0].Timestamp)

	quote := &entity.Quotes{
		Timestamp: make([]int64, count),
		Open:      make([]float64, count),
		Close:     make([]float64, count),
		High:      make([]float64, count),
		Low:       make([]float64, count),
		Volume:    make([]float64, count),
	}

	var factor float64
	for index := range r.Chart.Result[0].Timestamp {
		quote.Timestamp[index] = r.Chart.Result[0].Timestamp[index]

		if r.Chart.Result[0].Indicators.Adjclose[0].Adjclose[index] == 0 ||
			r.Chart.Result[0].Indicators.Quote[0].Close[index] == 0 {
			continue
		}

		factor = r.Chart.Result[0].Indicators.Adjclose[0].Adjclose[index] / r.Chart.Result[0].Indicators.Quote[0].Close[index]

		quote.Open[index] = r.Chart.Result[0].Indicators.Quote[0].Open[index] * factor
		quote.Close[index] = r.Chart.Result[0].Indicators.Quote[0].Close[index] * factor
		quote.High[index] = r.Chart.Result[0].Indicators.Quote[0].High[index] * factor
		quote.Low[index] = r.Chart.Result[0].Indicators.Quote[0].Low[index] * factor
		quote.Volume[index] = float64(r.Chart.Result[0].Indicators.Quote[0].Volume[index]) / factor
	}

	return quote
}
