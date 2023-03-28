package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/urfave/cli/v2"
	"io"
	"os"
)

const (
	BufSize          = (4 << 20) / 128 * 127
	varintSize       = 10
	nulRootCarHeader = "\x19" + // 25 bytes of CBOR (encoded as varint :cryingbear: )
		// map with 2 keys
		"\xA2" +
		// text-key with length 5
		"\x65" + "roots" +
		// 1 element array
		"\x81" +
		// tag 42
		"\xD8\x2A" +
		// bytes with length 5
		"\x45" +
		// nul-identity-cid prefixed with \x00 as required in DAG-CBOR: https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-cbor.md#links
		"\x00\x01\x55\x00\x00" +
		// text-key with length 7
		"\x67" + "version" +
		// 1, we call this v0 due to the nul-identity CID being an open question: https://github.com/ipld/go-car/issues/26#issuecomment-604299576
		"\x01"
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

	streamBuf := bufio.NewReaderSize(os.Stdin, BufSize)
	var streamLen int64

	maybeHeaderLen, err := streamBuf.Peek(varintSize)
	if err != nil {
		return fmt.Errorf("failed to read header: %s\n", err)
	}

	hdrLen, viLen := binary.Uvarint(maybeHeaderLen)
	if hdrLen <= 0 || viLen < 0 {
		return fmt.Errorf("unexpected header len = %d, varint len = %d\n", hdrLen, viLen)
	}

	actualViLen, err := io.CopyN(io.Discard, streamBuf, int64(viLen))
	if err != nil {
		return fmt.Errorf("failed to discard header varint: %s\n", err)
	}
	streamLen += actualViLen

	// ignoring header decoding for now
	actualHdrLen, err := io.CopyN(io.Discard, streamBuf, int64(hdrLen))
	if err != nil {
		return fmt.Errorf("failed to discard header header: %s\n", err)
	}
	streamLen += actualHdrLen

	for {
		maybeNextFrameLen, err := streamBuf.Peek(varintSize)
		if err == io.EOF {
			break
		}
		if err != nil && err != bufio.ErrBufferFull {
			return fmt.Errorf("unexpected error at offset %d: %s\n", streamLen, err)
		}
		if len(maybeNextFrameLen) == 0 {
			return fmt.Errorf("impossible 0-length peek without io.EOF at offset %d\n", streamLen)
		}

		var actualFrameLen int64
		var i int
		f := fmt.Sprintf("%s-%d.car", output, i)
		fi, err := os.Create(f)
		if err != nil {
			return fmt.Errorf("failed to create file: %s\n", err)
		}
		if _, err := io.WriteString(fi, nulRootCarHeader); err != nil {
			return fmt.Errorf("failed to write empty header: %s\n", err)
		}

		for actualFrameLen < int64(targetSize) {

			frameLen, viLen := binary.Uvarint(maybeNextFrameLen)
			if viLen <= 0 {
				// car file with trailing garbage behind it
				return fmt.Errorf("aborting car stream parse: undecodeable varint at offset %d", streamLen)
			}
			if frameLen > 2<<20 {
				// anything over ~2MiB got to be a mistake
				return fmt.Errorf("aborting car stream parse: unexpectedly large frame length of %d bytes at offset %d", frameLen, streamLen)
			}

			actualFrameLen, err = io.CopyN(fi, streamBuf, int64(viLen)+int64(frameLen))
			streamLen += actualFrameLen
			if err != nil {
				if err != io.EOF {
					return fmt.Errorf("unexpected error at offset %d: %s", streamLen-actualFrameLen, err)
				}
				break
			}
		}

		i++
	}

	return nil
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
