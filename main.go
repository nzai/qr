package main

import (
	"flag"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/schedulers"
	"github.com/nzai/qr/stores"
	"go.uber.org/zap"
)

var (
	startDate        = time.Now().AddDate(0, 0, -15) // yahoo finance limit
	storeArgument    = flag.String("s", "fs|/data", "store type: eg fs")
	exchangeArgument = flag.String("e", "Nyse", "exchange: eg Nyse,Nasdaq")
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	flag.Parse()

	store, err := stores.Parse(*storeArgument)
	if err != nil {
		zap.L().Fatal("parse store argument failed",
			zap.Error(err),
			zap.String("arg", *storeArgument))
	}
	defer store.Close()

	_exchanges, err := exchanges.Parse(*exchangeArgument)
	if err != nil {
		zap.L().Fatal("parse exchange argument failed",
			zap.Error(err),
			zap.String("arg", *exchangeArgument))
	}

	scheduler := schedulers.NewScheduler(store, _exchanges...)
	wg := scheduler.Run(startDate)
	wg.Wait()
}
