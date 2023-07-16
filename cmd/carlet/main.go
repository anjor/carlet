package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/anjor/carlet"
	"github.com/urfave/cli/v2"
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
			Usage:    "output filename prefix for car files.",
		},
	},
}

var splitAndCommPCmd = &cli.Command{
	Name:    "split-and-commp",
	Usage:   "Split CAR and calculate commp",
	Aliases: []string{"sac"},
	Action:  splitAndCommpAction,
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:     "size",
			Aliases:  []string{"s"},
			Required: true,
			Usage:    "Target size in bytes to chunk CARs to.",
		},
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: false,
			Usage:    "output filename prefix for car files.",
		},
		&cli.StringFlag{
			Name:     "metadata",
			Aliases:  []string{"m"},
			Required: false,
			Usage:    "optional metadata file name. Defaults to __metadata.csv",
			Value:    "__metadata.csv",
		},
	},
}

func splitAction(c *cli.Context) error {

	targetSize := c.Int("size")
	output := c.String("output")

	return carlet.SplitCar(os.Stdin, targetSize, output)
}

func splitAndCommpAction(c *cli.Context) error {
	size := c.Int("size")
	output := c.String("output")
	meta := c.String("metadata")

	carFiles, err := carlet.SplitAndCommp(os.Stdin, size, output)
	if err != nil {
		return err
	}

	f, err := os.Create(meta)
	defer f.Close()
	if err != nil {
		return err
	}

	w := csv.NewWriter(f)
	err = w.Write([]string{"timestamp", "filename prefix", "car file", "piece cid", "padded piece size"})
	if err != nil {
		return err
	}
	defer w.Flush()
	for _, c := range carFiles {
		err = w.Write([]string{
			time.Now().Format(time.RFC3339),
			output,
			c.Name,
			c.CommP.String(),
			strconv.FormatUint(c.PaddedSize, 10),
		})
	}
	return nil
}

func main() {

	app := cli.NewApp()
	app.Name = "carlet"
	app.Commands = []*cli.Command{
		splitCmd,
		splitAndCommPCmd,
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

}
