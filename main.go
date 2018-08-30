package main

import (
	"flag"
	"os"
	"time"

	"github.com/nzai/qr/sources"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/schedulers"
	"github.com/nzai/qr/stores"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	startDate        = time.Now().AddDate(0, 0, -60) // yahoo finance limit
	storeArgument    = flag.String("s", "fs:/data", "store type: eg fs")
	exchangeArgument = flag.String("e", "Nyse", "exchange: eg Nyse,Nasdaq")
)

func main() {
	logger := newLogger()
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

	_exchanges, err := exchanges.Parse(*exchangeArgument)
	if err != nil {
		zap.L().Fatal("parse exchange argument failed",
			zap.Error(err),
			zap.String("arg", *exchangeArgument))
	}

	scheduler := schedulers.NewScheduler(sources.NewYahooFinance(), store, _exchanges...)
	wg := scheduler.Run(startDate)
	wg.Wait()
}

// newLogger create new zap logger
func newLogger() *zap.Logger {

	infoPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.DebugLevel && lvl < zapcore.ErrorLevel
	})
	errorPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})

	consoleWriter := zapcore.Lock(os.Stdout)
	errorWriter := zapcore.Lock(os.Stderr)

	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, consoleWriter, infoPriority),
		zapcore.NewCore(consoleEncoder, errorWriter, errorPriority),
	)

	return zap.New(core, zap.AddCaller())
}
