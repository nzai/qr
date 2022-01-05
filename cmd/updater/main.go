package main

import (
	"log"
	"os"

	"github.com/nzai/qr/cmd/updater/command"
	"github.com/urfave/cli/v2"
)

func main() {
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
