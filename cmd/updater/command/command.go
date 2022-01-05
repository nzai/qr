package command

import (
	"github.com/urfave/cli/v2"
)

type Commander interface {
	Command() *cli.Command
}

var (
	Commands = []Commander{
		ShowVersion{},
		FetchData{},
	}
)

const (
	tdeDriverName = "taosSql"
)
