package main

import (
	"flag"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/stores"
	"go.uber.org/zap"
)

var (
	sourceStoreArgument = flag.String("src", "fs:/data", "source store type: eg fs")
	destStoreArgument   = flag.String("dest", "fs:/data", "dest store type: eg fs")
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

	sync := NewSync(sourceStore, destStore, _exchanges)
	wg := sync.Run()
	wg.Wait()
}
