package main

import (
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"
)

// Reference to the original bit-at-a-time decoder for a paired codec benchmark.
func decodePackedColumnsReference(data []byte) []byte {
	count, width := binary.Uvarint(data)
	data = data[width:]
	minimum := [4]uint16{uint16(data[0]), binary.LittleEndian.Uint16(data[1:]), binary.LittleEndian.Uint16(data[3:]), uint16(data[5])}
	widths := data[6:10]
	data = data[10:]
	out := make([]byte, int(count)*6)
	for field, bitWidth := range widths {
		size := (int(count)*int(bitWidth) + 7) / 8
		packed := data[:size]
		data = data[size:]
		for row := 0; row < int(count); row++ {
			var delta uint32
			for bit := 0; bit < int(bitWidth); bit++ {
				position := row*int(bitWidth) + bit
				delta |= uint32((packed[position/8]>>(position%8))&1) << bit
			}
			value := uint32(minimum[field]) + delta
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
	return out
}

func packedBenchPage(wide bool) []byte {
	rng := rand.New(rand.NewSource(19))
	docs := make([]document, 256)
	for i := range docs {
		docs[i] = document{Tenant: uint8(i % 4), Group: uint16(i % 128), Duration: uint16(i % 1000), Text: "alpha beta"}
		if wide {
			docs[i].Tenant = uint8(rng.Intn(256))
			docs[i].Group = uint16(rng.Intn(65536))
			docs[i].Duration = uint16(rng.Intn(65536))
		}
	}
	return encodePackedColumns(docs)
}

func TestWordPackedDecoderMatchesReference(t *testing.T) {
	for _, wide := range []bool{false, true} {
		page := packedBenchPage(wide)
		got, err := decodePackedColumns(page)
		if err != nil || !slices.Equal(got, decodePackedColumnsReference(page)) {
			t.Fatalf("wide=%v err=%v", wide, err)
		}
	}
}

func BenchmarkPackedDecode(b *testing.B) {
	for _, dataset := range []struct {
		name string
		wide bool
	}{{"narrow", false}, {"wide", true}} {
		page := packedBenchPage(dataset.wide)
		b.Run(dataset.name+"/reference", func(b *testing.B) {
			for range b.N {
				_ = decodePackedColumnsReference(page)
			}
		})
		b.Run(dataset.name+"/word", func(b *testing.B) {
			for range b.N {
				if _, err := decodePackedColumns(page); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
