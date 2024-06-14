package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

func init() {
	RegisterCommand(&ShowVersion{})
}

type ShowVersion struct{}

func (c ShowVersion) Command() *cli.Command {
	return &cli.Command{
		Name:    "version",
		Aliases: []string{"v"},
		Action: func(ctx context.Context, c *cli.Command) error {
			fmt.Println("v1.0.0")
			return nil
		},
	}
}
