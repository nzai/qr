package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	app := &cli.App{
		Name:      "cli",
		Usage:     "a quote cli",
		UsageText: "cli [global options] command [command options] [arguments...]",
		HideHelp:  true,
		Commands: []*cli.Command{
			showVersion{}.Command(),
			new(rollup).Command(),
		},
	}

	// timeout duration: 30s
	ctx, _ := context.WithTimeout(context.Background(), time.Minute*30)
	err := app.RunContext(ctx, os.Args)
	if err != nil {
		fmt.Println("\033[31m" + err.Error() + "\033[0m ")
		return
	}
}
