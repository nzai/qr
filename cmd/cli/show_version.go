package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

type showVersion struct{}

func (s showVersion) Command() *cli.Command {
	return &cli.Command{
		Name:    "version",
		Usage:   "show version",
		Aliases: []string{"v"},
		Action: func(c *cli.Context) error {
			fmt.Println("v0.1.0")
			return nil
		},
	}
}
