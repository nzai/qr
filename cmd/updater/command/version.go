package command

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

type ShowVersion struct{}

func (c ShowVersion) Command() *cli.Command {
	return &cli.Command{
		Name:    "version",
		Aliases: []string{"v"},
		Action: func(c *cli.Context) error {
			fmt.Println("v1.0.0")
			return nil
		},
	}
}
