package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"math/bits"
	"slices"
	"strings"
	"testing"
)

// These are deliberately small, independent probes, not new segment formats.
// The block codec is fixed-width packing, not partitioned Elias-Fano or SIMD.
func blockPostings(list []posting) []byte {
	var out []byte
	prev := -1
	for start := 0; start < len(list); start += 128 {
		block := list[start:min(start+128, len(list))]
		var maxDelta uint32
		for _, p := range block {
			maxDelta = max(maxDelta, uint32(p.id-prev))
			prev = p.id
		}
		width := bits.Len32(maxDelta)
		out = append(out, byte(len(block)-1), byte(width))
		packed := make([]byte, (len(block)*width+7)/8)
		if start == 0 {
			prev = -1
		} else {
			prev = list[start-1].id
		}
		for i, p := range block {
			delta := uint64(p.id - prev)
			bit := i * width
			for j := 0; j < width; j++ {
				packed[(bit+j)/8] |= byte((delta>>j)&1) << ((bit + j) % 8)
			}
			prev = p.id
		}
		out = append(out, packed...)
		for _, p := range block {
			out = append(out, byte(p.tf))
		}
	}
	return out
}

func decodeBlockPostings(data []byte) []posting {
	var out []posting
	prev := -1
	for len(data) > 0 {
		count, width := int(data[0])+1, int(data[1])
		data = data[2:]
		packedLen := (count*width + 7) / 8
		packed, tf := data[:packedLen], data[packedLen:packedLen+count]
		data = data[packedLen+count:]
		for i := 0; i < count; i++ {
			var delta uint64
			bit := i * width
			for j := 0; j < width; j++ {
				delta |= uint64((packed[(bit+j)/8]>>((bit+j)%8))&1) << j
			}
			prev += int(delta)
			out = append(out, posting{id: prev, tf: int(tf[i])})
		}
	}
	return out
}

func varintPostings(list []posting) []byte {
	var out []byte
	prev := -1
	for _, p := range list {
		out = binary.AppendUvarint(out, uint64(p.id-prev))
		out = binary.AppendUvarint(out, uint64(p.tf))
		prev = p.id
	}
	return out
}

func decodeVarintPostings(data []byte) []posting {
	var out []posting
	prev := -1
	for len(data) > 0 {
		delta, n := binary.Uvarint(data)
		data = data[n:]
		tf, n := binary.Uvarint(data)
		data = data[n:]
		prev += int(delta)
		out = append(out, posting{id: prev, tf: int(tf)})
	}
	return out
}

func compressedSize(data []byte) int {
	var out bytes.Buffer
	zw := zlib.NewWriter(&out)
	_, _ = zw.Write(data)
	_ = zw.Close()
	return out.Len()
}

func bsi(values []uint16) [][]uint64 {
	planes := make([][]uint64, 16)
	for bit := range planes {
		planes[bit] = make([]uint64, (len(values)+63)/64)
	}
	for id, value := range values {
		for bit := range planes {
			planes[bit][id/64] |= uint64((value>>bit)&1) << (id % 64)
		}
	}
	return planes
}

func sumBSI(planes [][]uint64, mask []uint64) uint64 {
	var sum uint64
	for bit, plane := range planes {
		for i, word := range plane {
			sum += uint64(bits.OnesCount64(word&mask[i])) << bit
		}
	}
	return sum
}

var literatureSink uint64

func TestLiteratureAlternatives(t *testing.T) {
	for _, fixture := range []struct {
		profile string
		count   int
	}{{"baseline", 100000}, {"noisy", 50000}, {"clustered", 100000}, {"wide_fields", 100000}} {
		docs, err := matrixDocuments(fixture.count, 4093, fixture.profile)
		if err != nil {
			t.Fatal(err)
		}
		lists := make(map[string][]posting)
		values := make([]uint16, len(docs))
		for id, doc := range docs {
			values[id] = doc.Duration
			for term, tf := range frequencies(tokenize(doc.Text)) {
				lists[term] = append(lists[term], posting{id: id, tf: tf})
			}
		}
		var varBytes, blockBytes, varZlib, blockZlib int
		for _, list := range lists {
			baseline, candidate := varintPostings(list), blockPostings(list)
			if !slices.Equal(list, decodeVarintPostings(baseline)) || !slices.Equal(list, decodeBlockPostings(candidate)) {
				t.Fatal("block postings roundtrip")
			}
			varBytes += len(baseline)
			blockBytes += len(candidate)
			varZlib += partSize(baseline)
			blockZlib += partSize(candidate)
		}
		planes := bsi(values)
		var raw bytes.Buffer
		for _, value := range values {
			_ = binary.Write(&raw, binary.LittleEndian, value)
		}
		var bitsRaw bytes.Buffer
		for _, plane := range planes {
			for _, word := range plane {
				_ = binary.Write(&bitsRaw, binary.LittleEndian, word)
			}
		}
		t.Logf("profile=%s docs=%d terms=%d postings varint=%d block=%d varintStored=%d blockStored=%d duration raw=%d bsi=%d rawZlib=%d bsiZlib=%d", fixture.profile, len(docs), len(lists), varBytes, blockBytes, varZlib, blockZlib, raw.Len(), bitsRaw.Len(), compressedSize(raw.Bytes()), compressedSize(bitsRaw.Bytes()))
		for _, term := range []string{"fatal", "timeout", "request"} {
			list := lists[term]
			mask := make([]uint64, (len(docs)+63)/64)
			var expected uint64
			for _, p := range list {
				mask[p.id/64] |= 1 << (p.id % 64)
				expected += uint64(values[p.id])
			}
			if got := sumBSI(planes, mask); got != expected {
				t.Fatalf("%s/%s sum=%d want=%d", fixture.profile, term, got, expected)
			}
			if fixture.profile != "baseline" {
				continue
			}
			baseBytes, blockBytes := varintPostings(list), blockPostings(list)
			varintDecode := testing.Benchmark(func(b *testing.B) {
				for range b.N {
					literatureSink = uint64(len(decodeVarintPostings(baseBytes)))
				}
			})
			blockDecode := testing.Benchmark(func(b *testing.B) {
				for range b.N {
					literatureSink = uint64(len(decodeBlockPostings(blockBytes)))
				}
			})
			t.Logf("decode profile=%s term=%s matches=%d varintNs=%d blockNs=%d", fixture.profile, term, len(list), varintDecode.NsPerOp(), blockDecode.NsPerOp())
			ids := testing.Benchmark(func(b *testing.B) {
				for range b.N {
					var sum uint64
					for _, p := range list {
						sum += uint64(values[p.id])
					}
					literatureSink = sum
				}
			})
			bitmap := testing.Benchmark(func(b *testing.B) {
				for range b.N {
					literatureSink = sumBSI(planes, mask)
				}
			})
			t.Logf("sum profile=%s term=%s matches=%d idsNs=%d bsiNs=%d", fixture.profile, term, len(list), ids.NsPerOp(), bitmap.NsPerOp())
		}
	}
}

func TestTopKPruningCannotProduceExactAggregates(t *testing.T) {
	// The second block cannot beat Top-1's score, but both rows still match.
	scores := []int{100, 1, 1}
	durations := []int{1, 10, 20}
	if scores[1] >= scores[0] || scores[2] >= scores[0] {
		t.Fatal("fixture does not permit safe Top-1 pruning")
	}
	var fullSum int
	for _, duration := range durations {
		fullSum += duration
	}
	if len(durations) != 3 || fullSum != 31 || durations[0] == fullSum {
		t.Fatal("Top-1-only aggregate unexpectedly equals the exact aggregate")
	}
}

func TestTrigramCandidates(t *testing.T) {
	for _, profile := range []string{"baseline", "noisy"} {
		docs, err := matrixDocuments(20000, 4093, profile)
		if err != nil {
			t.Fatal(err)
		}
		grams := make(map[string][]posting)
		var source []byte
		for id, doc := range docs {
			source = append(source, doc.Text...)
			seen := make(map[string]bool)
			for i := 0; i+3 <= len(doc.Text); i++ {
				gram := doc.Text[i : i+3]
				if !seen[gram] {
					grams[gram] = append(grams[gram], posting{id: id, tf: 1})
					seen[gram] = true
				}
			}
		}
		var indexBytes int
		for _, list := range grams {
			indexBytes += partSize(varintPostings(list))
		}
		t.Logf("trigram profile=%s docs=%d gramTerms=%d postingBytes=%d sourceZlib=%d", profile, len(docs), len(grams), indexBytes, compressedSize(source))
		for _, needle := range []string{"timeout", "service", "trace", "aabb"} {
			var list []posting
			first := true
			for i := 0; i+3 <= len(needle); i++ {
				candidate := grams[needle[i:i+3]]
				if first || len(candidate) < len(list) {
					list = candidate
					first = false
				}
			}
			var got, want int
			for _, p := range list {
				if strings.Contains(docs[p.id].Text, needle) {
					got++
				}
			}
			for _, doc := range docs {
				if strings.Contains(doc.Text, needle) {
					want++
				}
			}
			if got != want {
				t.Fatalf("%s/%s candidates omitted matches: %d != %d", profile, needle, got, want)
			}
			t.Logf("trigram profile=%s literal=%s candidates=%d exact=%d", profile, needle, len(list), got)
		}
	}
}
