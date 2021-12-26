package main

import (
	"context"
	"flag"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/stores"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

var (
	sourceStoreArgument = flag.String("src", "fs|/data", "source store type: eg fs")
	destStoreArgument   = flag.String("dest", "fs|/data", "dest store type: eg fs")
	exchangeArgument    = flag.String("e", "Nyse", "exchange: eg Nyse,Nasdaq")
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	flag.Parse()

	sourceStore, err := stores.Parse(*sourceStoreArgument)
	if err != nil {
		zap.L().Fatal("parse source store argument failed",
			zap.Error(err),
			zap.String("arg", *sourceStoreArgument))
	}
	defer sourceStore.Close()

	destStore, err := stores.Parse(*destStoreArgument)
	if err != nil {
		zap.L().Fatal("parse dest store argument failed",
			zap.Error(err),
			zap.String("arg", *destStoreArgument))
	}
	defer destStore.Close()

	_exchanges, err := exchanges.Parse(*exchangeArgument)
	if err != nil {
		zap.L().Fatal("parse exchange argument failed",
			zap.Error(err),
			zap.String("arg", *exchangeArgument))
	}

	zap.L().Info("sync start")
	defer zap.L().Info("sync end")

	ch := make(chan *EDQ, 4)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	go func() {
		defer close(ch)
		loader(ctx, sourceStore, destStore, _exchanges, ch)
	}()

	saver(destStore, ch)
}

func loader(ctx context.Context, source, dest stores.Store, _exchanges []exchanges.Exchange, ch chan *EDQ) {
	for _, exchange := range _exchanges {
		startTime := time.Now()

		date := time.Date(2015, 5, 1, 0, 0, 0, 0, exchange.Location())
		endDate := utils.TodayZero(startTime.In(exchange.Location()))

		var exists bool
		var edq *quotes.ExchangeDailyQuote
		var err error
		for date.Before(endDate) {
			exists, err = dest.Exists(exchange, date)
			if err != nil {
				zap.L().Error("check edq exists failed",
					zap.Error(err),
					zap.Any("exchange", exchange.Code()),
					zap.Any("date", date.Format("2006-01-02")))
				return
			}

			if exists {
				date = date.AddDate(0, 0, 1)
				continue
			}

			edq, err = source.Load(exchange, date)
			if err != nil {
				zap.L().Error("load edq failed",
					zap.Error(err),
					zap.Any("exchange", exchange.Code()),
					zap.Any("date", date.Format("2006-01-02")))
				return
			}

			zap.L().Info("load edq success",
				zap.Any("exchange", exchange.Code()),
				zap.Any("date", date.Format("2006-01-02")))

			ch <- &EDQ{
				Exchange: exchange,
				Date:     date,
				Quote:    edq,
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			date = date.AddDate(0, 0, 1)
		}
	}

	zap.L().Info("load complete")
}

func saver(dest stores.Store, ch chan *EDQ) {
	var err error
	for edq := range ch {
		err = dest.Save(edq.Exchange, edq.Date, edq.Quote)
		if err != nil {
			zap.L().Error("save edq failed",
				zap.Error(err),
				zap.Any("exchange", edq.Exchange.Code()),
				zap.Any("date", edq.Date.Format("2006-01-02")))
			return
		}

		zap.L().Info("save edq success",
			zap.Any("exchange", edq.Exchange.Code()),
			zap.Any("date", edq.Date.Format("2006-01-02")))
	}

	zap.L().Info("save complete")
}

type EDQ struct {
	Exchange exchanges.Exchange
	Date     time.Time
	Quote    *quotes.ExchangeDailyQuote
}
