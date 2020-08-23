package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/nzai/qr/config"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/schedulers"
	"github.com/nzai/qr/stores"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	configPath = flag.String("c", "config.toml", "toml config file path")
	logPath    = flag.String("log", "log.txt", "log file path")
)

func main() {
	flag.Parse()

	logger, err := initLogger(*logPath)
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	// read config from file
	conf, err := config.Parse(*configPath)
	if err != nil {
		zap.L().Fatal("read config failed", zap.Error(err))
	}

	err = utils.GetWeChatService().SendMessage("qr started")
	if err != nil {
		zap.L().Fatal("send start message failed", zap.Error(err))
	}
	zap.L().Debug("send start message success")

	startDate := time.Now().AddDate(0, 0, -conf.LastDays) // yahoo finance limit

	store, err := stores.Parse(conf.Stores)
	if err != nil {
		zap.L().Fatal("parse store argument failed",
			zap.Error(err),
			zap.String("arg", conf.Stores))
	}
	defer store.Close()

	// notifier := notifiers.NewNsq(conf.Nsq.Broker, conf.Nsq.TLSCert, conf.Nsq.TLSKey, conf.Nsq.Topic)
	// defer notifier.Close()

	// zap.L().Info("init nsq notifier success",
	// 	zap.String("broker", conf.Nsq.Broker),
	// 	zap.String("topic", conf.Nsq.Topic))

	_exchanges, err := exchanges.Parse(conf.Exchanges)
	if err != nil {
		zap.L().Fatal("parse exchange argument failed",
			zap.Error(err),
			zap.String("arg", conf.Exchanges))
	}

	scheduler := schedulers.NewScheduler(store, _exchanges...)
	wg := scheduler.Run(startDate)
	wg.Wait()
}

func initLogger(logPath string) (*zap.Logger, error) {
	infoPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.InfoLevel
	})
	debugPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.DebugLevel
	})

	consoleWriter := zapcore.Lock(os.Stdout)

	fileWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    100, // megabytes
		MaxBackups: 10,
		MaxAge:     30, // days
	})

	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	fileEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, consoleWriter, infoPriority),
		zapcore.NewCore(fileEncoder, fileWriter, debugPriority),
	)

	return zap.New(core, zap.AddCaller()), nil
}
