package media

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// exifScanLimit bounds how much of a JPEG is read looking for the APP1
// segment. Cameras write EXIF right after SOI; anything past the first 64KiB
// is image data.
const exifScanLimit = 64 << 10

// JPEGOrientation returns the EXIF Orientation tag (1..8) of a JPEG file, or
// 0 when the file has no usable EXIF orientation. It only inspects the JPEG
// segment structure, never decodes image data, and is byte-order aware.
// Non-JPEG input yields 0, not an error; errors are I/O failures only.
func JPEGOrientation(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, exifScanLimit)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return 0, err
	}
	return jpegOrientation(buf[:n]), nil
}

// jpegOrientation walks JPEG marker segments in data until SOS/EOI and parses
// the first APP1 Exif segment's IFD0 for tag 0x0112.
func jpegOrientation(data []byte) int {
	if len(data) < 4 || data[0] != 0xFF || data[1] != 0xD8 {
		return 0
	}
	pos := 2
	for pos+4 <= len(data) {
		if data[pos] != 0xFF {
			return 0
		}
		marker := data[pos+1]
		switch {
		case marker == 0xFF: // fill byte
			pos++
			continue
		case marker == 0xD8 || (marker >= 0xD0 && marker <= 0xD7) || marker == 0x01:
			pos += 2 // standalone markers without a length
			continue
		case marker == 0xDA || marker == 0xD9: // SOS / EOI: no more metadata
			return 0
		}
		segLen := int(binary.BigEndian.Uint16(data[pos+2:]))
		if segLen < 2 {
			return 0
		}
		start, end := pos+4, pos+2+segLen
		if end > len(data) {
			end = len(data)
		}
		if marker == 0xE1 && end-start > 6 && string(data[start:start+6]) == "Exif\x00\x00" {
			return tiffOrientation(data[start+6 : end])
		}
		pos = pos + 2 + segLen
	}
	return 0
}

// tiffOrientation parses a TIFF header plus IFD0 and returns Orientation.
func tiffOrientation(tiff []byte) int {
	if len(tiff) < 8 {
		return 0
	}
	var bo binary.ByteOrder
	switch string(tiff[:2]) {
	case "II":
		bo = binary.LittleEndian
	case "MM":
		bo = binary.BigEndian
	default:
		return 0
	}
	if bo.Uint16(tiff[2:]) != 42 {
		return 0
	}
	ifd := int(bo.Uint32(tiff[4:]))
	if ifd < 8 || ifd+2 > len(tiff) {
		return 0
	}
	count := int(bo.Uint16(tiff[ifd:]))
	for i := range count {
		e := ifd + 2 + i*12
		if e+12 > len(tiff) {
			return 0
		}
		tag := bo.Uint16(tiff[e:])
		typ := bo.Uint16(tiff[e+2:])
		if tag != 0x0112 {
			continue
		}
		if typ != 3 { // SHORT
			return 0
		}
		v := int(bo.Uint16(tiff[e+8:]))
		if v < 1 || v > 8 {
			return 0
		}
		return v
	}
	return 0
}
