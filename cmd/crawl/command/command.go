package command

import (
	"github.com/urfave/cli/v3"
)

type Commander interface {
	Command() *cli.Command
}

var Commands = []Commander{}

func RegisterCommand(cmd Commander) {
	Commands = append(Commands, cmd)
}
