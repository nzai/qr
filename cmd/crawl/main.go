package main

import (
	"context"
	"os"

	"github.com/nzai/log"
	"github.com/nzai/qr/cmd/crawl/command"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	lc := zap.NewDevelopmentConfig()
	lc.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	logger, _ := lc.Build()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	log.ReplaceGlobals(log.New(log.WithLogLevel(log.LevelInfo)))

	app := &cli.Command{
		Name:  "crawl",
		Usage: "make it easier to get stock data",
	}

	for _, command := range command.Commands {
		app.Commands = append(app.Commands, command.Command())
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		zap.L().Fatal(err.Error())
	}
}
