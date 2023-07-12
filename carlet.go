package carlet

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
)

const (
	bufSize          = (4 << 20) / 128 * 127
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
	maxBlockSize = 2 << 20 // 2 MiB
)

type CarFile struct {
	Name       string
	CommP      cid.Cid
	PaddedSize uint64
}

// SplitCar splits a car file into smaller car files of the specified target size
func SplitCar(rdr io.Reader, targetSize int, namePrefix string) error {

	streamBuf := bufio.NewReaderSize(rdr, bufSize)
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

	var i int
	for {
		f := fmt.Sprintf("%s%d.car", namePrefix, i)
		fmt.Printf("Writing file: %s\n", f)
		fi, err := os.Create(f)
		if err != nil {
			return fmt.Errorf("failed to create file: %s\n", err)
		}
		if _, err := io.WriteString(fi, nulRootCarHeader); err != nil {
			return fmt.Errorf("failed to write empty header: %s\n", err)
		}

		var carletLen int64
		for carletLen < int64(targetSize) {
			maybeNextFrameLen, err := streamBuf.Peek(varintSize)
			if err == io.EOF {
				return nil
			}
			if err != nil && err != bufio.ErrBufferFull {
				return fmt.Errorf("unexpected error at offset %d: %s\n", streamLen, err)
			}
			if len(maybeNextFrameLen) == 0 {
				return fmt.Errorf("impossible 0-length peek without io.EOF at offset %d\n", streamLen)
			}

			frameLen, viL := binary.Uvarint(maybeNextFrameLen)
			if viL <= 0 {
				// car file with trailing garbage behind it
				return fmt.Errorf("aborting car stream parse: undecodeable varint at offset %d", streamLen)
			}
			if frameLen > 2<<20 {
				// anything over ~2MiB got to be a mistake
				return fmt.Errorf("aborting car stream parse: unexpectedly large frame length of %d bytes at offset %d", frameLen, streamLen)
			}

			actualFrameLen, err := io.CopyN(fi, streamBuf, int64(viL)+int64(frameLen))
			streamLen += actualFrameLen
			carletLen += actualFrameLen
			if err != nil {
				if err != io.EOF {
					return fmt.Errorf("unexpected error at offset %d: %s", streamLen-actualFrameLen, err)
				}
				return nil
			}
		}

		fi.Close()
		i++
	}
}

// SplitAndCommp splits a car file into smaller car files but also calculates commP at the same time.
func SplitAndCommp(r io.Reader, targetSize int, namePrefix string) ([]CarFile, error) {
	var carFiles []CarFile

	streamBuf := bufio.NewReaderSize(r, bufSize)
	var streamLen int64

	streamLen, err := discardHeader(streamBuf, streamLen)
	if err != nil {
		return carFiles, err
	}

	var i int
	for {
		fname := fmt.Sprintf("%s%d.car", namePrefix, i)
		fi, err := os.Create(fname)
		cp := new(commp.Calc)

		w := io.MultiWriter(fi, cp)

		if err != nil {
			return carFiles, fmt.Errorf("failed to create file: %s\n", err)
		}
		if _, err := io.WriteString(w, nulRootCarHeader); err != nil {
			return carFiles, fmt.Errorf("failed to write empty header: %s\n", err)
		}

		var carletLen int64
		for carletLen < int64(targetSize) {
			maybeNextFrameLen, err := streamBuf.Peek(varintSize)
			if err == io.EOF {
				carFile, err := cleanup(cp, namePrefix, i, fi)
				if err != nil {
					return carFiles, err
				}
				carFiles = append(carFiles, carFile)

				return carFiles, nil
			}
			if err != nil && err != bufio.ErrBufferFull {
				return carFiles, fmt.Errorf("unexpected error at offset %d: %s\n", streamLen, err)
			}
			if len(maybeNextFrameLen) == 0 {
				return carFiles, fmt.Errorf("impossible 0-length peek without io.EOF at offset %d\n", streamLen)
			}

			frameLen, viL := binary.Uvarint(maybeNextFrameLen)
			if viL <= 0 {
				// car file with trailing garbage behind it
				return carFiles, fmt.Errorf("aborting car stream parse: undecodeable varint at offset %d", streamLen)
			}
			if frameLen > maxBlockSize {
				// anything over ~2MiB got to be a mistake
				return carFiles, fmt.Errorf("aborting car stream parse: unexpectedly large frame length of %d bytes at offset %d", frameLen, streamLen)
			}

			actualFrameLen, err := io.CopyN(w, streamBuf, int64(viL)+int64(frameLen))
			streamLen += actualFrameLen
			carletLen += actualFrameLen
			if err != nil {
				if err != io.EOF {
					return carFiles, fmt.Errorf("unexpected error at offset %d: %s", streamLen-actualFrameLen, err)
				}
				carFile, err := cleanup(cp, namePrefix, i, fi)
				if err != nil {
					return carFiles, err
				}
				carFiles = append(carFiles, carFile)

				return carFiles, nil
			}
		}

		carFile, err := cleanup(cp, namePrefix, i, fi)
		if err != nil {
			return carFiles, err
		}
		carFiles = append(carFiles, carFile)

		err = resetCP(cp)
		if err != nil {
			return carFiles, err
		}
		i++
	}
}

func discardHeader(streamBuf *bufio.Reader, streamLen int64) (int64, error) {
	maybeHeaderLen, err := streamBuf.Peek(varintSize)
	if err != nil {
		return 0, fmt.Errorf("failed to read header: %s\n", err)
	}

	hdrLen, viLen := binary.Uvarint(maybeHeaderLen)
	if hdrLen <= 0 || viLen < 0 {
		return 0, fmt.Errorf("unexpected header len = %d, varint len = %d\n", hdrLen, viLen)
	}

	actualViLen, err := io.CopyN(io.Discard, streamBuf, int64(viLen))
	if err != nil {
		return 0, fmt.Errorf("failed to discard header varint: %s\n", err)
	}
	streamLen += actualViLen

	// ignoring header decoding for now
	actualHdrLen, err := io.CopyN(io.Discard, streamBuf, int64(hdrLen))
	if err != nil {
		return 0, fmt.Errorf("failed to discard header header: %s\n", err)
	}
	streamLen += actualHdrLen

	return streamLen, nil
}

func resetCP(cp *commp.Calc) error {
	cp.Reset()
	_, err := cp.Write([]byte(nulRootCarHeader))

	return err
}

func cleanup(cp *commp.Calc, namePrefix string, i int, f *os.File) (CarFile, error) {
	rawCommP, paddedSize, err := cp.Digest()
	if err != nil {
		return CarFile{}, err
	}

	commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
	if err != nil {
		return CarFile{}, err
	}

	f.Close()
	oldn := fmt.Sprintf("%s%d.car", namePrefix, i)
	newn := fmt.Sprintf("%s%s.car", namePrefix, commCid)
	err = os.Rename(oldn, newn)
	if err != nil {
		return CarFile{}, err
	}

	return CarFile{
		Name:       newn,
		CommP:      commCid,
		PaddedSize: paddedSize,
	}, nil
}
