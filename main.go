package main

import (
	"flag"
	"time"

	"github.com/nzai/qr/config"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/schedulers"
	"github.com/nzai/qr/stores"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

var (
	configPath = flag.String("c", "config.toml", "toml config file path")
)

func main() {
	conf := zap.NewDevelopmentConfig()
	conf.DisableStacktrace = true

	logger, _ := conf.Build()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	flag.Parse()

	// read config from file
	_, err := config.Parse(*configPath)
	if err != nil {
		zap.L().Fatal("read config failed", zap.Error(err))
	}

	err = utils.GetWeChatService().SendMessage("qr started")
	if err != nil {
		zap.L().Fatal("send start message failed", zap.Error(err))
	}
	zap.L().Debug("send start message success")

	startDate := time.Now().AddDate(0, 0, -config.Get().LastDays) // yahoo finance limit

	store, err := stores.Parse(config.Get().Stores)
	if err != nil {
		zap.L().Fatal("parse store argument failed",
			zap.Error(err),
			zap.String("arg", config.Get().Stores))
	}
	defer store.Close()

	_exchanges, err := exchanges.Parse(config.Get().Exchanges)
	if err != nil {
		zap.L().Fatal("parse exchange argument failed",
			zap.Error(err),
			zap.String("arg", config.Get().Exchanges))
	}

	scheduler := schedulers.NewScheduler(store, _exchanges...)
	wg := scheduler.Run(startDate)
	wg.Wait()
}
