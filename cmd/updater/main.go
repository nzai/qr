package main

import (
	"log"
	"os"

	"github.com/nzai/qr/cmd/updater/command"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	app := &cli.App{
		Name:     "updater",
		Commands: []*cli.Command{},
	}

	for _, command := range command.Commands {
		app.Commands = append(app.Commands, command.Command())
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
