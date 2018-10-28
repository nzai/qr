package stores

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// Cos define tencent cos store
type Cos struct {
	client *cos.Client
}

// NewCos create tencent cos store
func NewCos(client *cos.Client) *Cos {
	return &Cos{client: client}
}

// storePath return store path
func (s Cos) storePath(exchange exchanges.Exchange, date time.Time) string {
	return fmt.Sprintf("%s/%s", date.Format("2006/01/02"), exchange.Code())
}

// Exists check quote exists
func (s Cos) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	_, err := s.client.Object.Head(context.Background(), s.storePath(exchange, date), nil)
	return err == nil, nil
}

// Save save exchange daily quote
func (s Cos) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	buffer := new(bytes.Buffer)
	// init gzip writer
	gw, err := gzip.NewWriterLevel(buffer, gzip.BestCompression)
	if err != nil {
		zap.L().Error("create gzip writer failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	// encode to gzip writer
	err = edq.Encode(gw)
	if err != nil {
		zap.L().Error("encode quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	gw.Flush()
	gw.Close()

	_, err = s.client.Object.Put(context.Background(), s.storePath(exchange, date), buffer, nil)
	if err != nil {
		zap.L().Error("put exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

// Load load exchange daily quote
func (s Cos) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	response, err := s.client.Object.Get(context.Background(), s.storePath(exchange, date), nil)
	if err != nil {
		zap.L().Error("get exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}
	defer response.Body.Close()

	// init gzip reader
	gr, err := gzip.NewReader(response.Body)
	if err != nil {
		zap.L().Error("create gzip reader failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}
	defer gr.Close()

	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, gr)
	if err != nil {
		zap.L().Error("get exchange daily quote response failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	// decode from bytes
	edq := new(quotes.ExchangeDailyQuote)
	err = edq.Decode(buffer)
	if err != nil {
		zap.L().Error("decode exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	return edq, nil
}

// Close close store
func (s Cos) Close() error {
	return nil
}
