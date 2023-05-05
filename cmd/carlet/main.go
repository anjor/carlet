package main

import (
	"fmt"
	"github.com/anjor/carlet"
	"github.com/urfave/cli/v2"
	"os"
)

var splitCmd = &cli.Command{
	Name:   "split",
	Action: splitAction,
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:     "size",
			Aliases:  []string{"s"},
			Value:    1024 * 1024,
			Usage:    "Target size in bytes to chunk CARs to.",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: false,
			Usage:    "output name for car files",
		},
	},
}

func splitAction(c *cli.Context) error {

	targetSize := c.Int("size")
	output := c.String("output")

	return carlet.SplitCar(os.Stdin, targetSize, output)
}

func main() {

	app := cli.NewApp()
	app.Name = "carlet"
	app.Commands = []*cli.Command{splitCmd}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

}
