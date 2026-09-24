package main

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
)

// Each page stores a per-field minimum and only the bits needed for deltas.
// The four fields remain a single Range-addressable page; decoding restores
// the baseline six-byte row shape consumed by both query paths.
func encodePackedColumns(docs []document) []byte {
	values := [4][]uint16{}
	minimum := [4]uint16{255, 65535, 65535, 255}
	maximum := [4]uint16{}
	for _, d := range docs {
		row := [4]uint16{uint16(d.Tenant), d.Group, d.Duration, uint16(len(tokenize(d.Text)))}
		for field, value := range row {
			values[field] = append(values[field], value)
			minimum[field] = min(minimum[field], value)
			maximum[field] = max(maximum[field], value)
		}
	}
	data := binary.AppendUvarint(nil, uint64(len(docs)))
	data = append(data, byte(minimum[0]))
	data = binary.LittleEndian.AppendUint16(data, minimum[1])
	data = binary.LittleEndian.AppendUint16(data, minimum[2])
	data = append(data, byte(minimum[3]))
	var widths [4]int
	for field := range widths {
		widths[field] = bits.Len16(maximum[field] - minimum[field])
		data = append(data, byte(widths[field]))
	}
	for field, width := range widths {
		start := len(data)
		data = append(data, make([]byte, (len(docs)*width+7)/8)...)
		for row, value := range values[field] {
			delta := value - minimum[field]
			for bit := 0; bit < width; bit++ {
				position := row*width + bit
				data[start+position/8] |= byte((delta>>bit)&1) << (position % 8)
			}
		}
	}
	return data
}

func decodePackedColumns(data []byte) ([]byte, error) {
	count, width := binary.Uvarint(data)
	if width <= 0 || count == 0 || count > 1_000_000 || len(data)-width < 10 {
		return nil, errors.New("invalid packed column header")
	}
	data = data[width:]
	minimum := [4]uint16{uint16(data[0]), binary.LittleEndian.Uint16(data[1:]), binary.LittleEndian.Uint16(data[3:]), uint16(data[5])}
	widths := data[6:10]
	data = data[10:]
	out := make([]byte, int(count)*6)
	for field, bitWidth := range widths {
		if bitWidth > 16 || (field == 0 || field == 3) && bitWidth > 8 {
			return nil, errors.New("invalid packed column width")
		}
		size := (int(count)*int(bitWidth) + 7) / 8
		if size > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		packed := data[:size]
		data = data[size:]
		for row := 0; row < int(count); row++ {
			var delta uint32
			for bit := 0; bit < int(bitWidth); bit++ {
				position := row*int(bitWidth) + bit
				delta |= uint32((packed[position/8]>>(position%8))&1) << bit
			}
			value := uint32(minimum[field]) + delta
			if value > 65535 || (field == 0 || field == 3) && value > 255 {
				return nil, errors.New("packed column overflow")
			}
			slot := out[row*6:]
			switch field {
			case 0:
				slot[0] = byte(value)
			case 1:
				binary.LittleEndian.PutUint16(slot[1:], uint16(value))
			case 2:
				binary.LittleEndian.PutUint16(slot[3:], uint16(value))
			case 3:
				slot[5] = byte(value)
			}
		}
	}
	if len(data) != 0 {
		return nil, errors.New("trailing packed column bytes")
	}
	return out, nil
}
