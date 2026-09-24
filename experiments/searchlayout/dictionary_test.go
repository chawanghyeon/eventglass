package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"io"
	"slices"
	"testing"
)

// A one-segment block dictionary probe. It tests the bytes that would replace
// a resident term map, not S3 latency or the production manifest format.
func TestBlockDictionaryWithInlinePostings(t *testing.T) {
	for _, profile := range []string{"baseline", "noisy"} {
		docs, err := matrixDocuments(50000, 4093, profile)
		if err != nil {
			t.Fatal(err)
		}
		lists := make(map[string][]posting)
		for id, doc := range docs {
			for term, tf := range frequencies(tokenize(doc.Text)) {
				lists[term] = append(lists[term], posting{id: id, tf: tf})
			}
		}
		terms := make([]string, 0, len(lists))
		for term := range lists {
			terms = append(terms, term)
		}
		slices.Sort(terms)
		old := corpus{Count: len(docs), AvgLen: 7, DF: make(map[string]int, len(terms))}
		seg := segment{Name: "segment.bin", Base: 0, Count: len(docs), PageDocs: 256, SHA256: hex.EncodeToString(make([]byte, 32)), Postings: make(map[string]part, len(terms))}
		var oldPostings, externalBytes int
		for _, term := range terms {
			encoded := varintPostings(lists[term])
			size := partSize(encoded)
			old.DF[term] = len(lists[term])
			seg.Postings[term] = part{Offset: int64(oldPostings), Size: int64(size), CRC32C: crc32.Checksum(encoded, crcTable), Zlib: len(encoded) >= 64}
			oldPostings += size
			if len(lists[term]) > 6 {
				externalBytes += size
			}
		}
		old.Segs = []segment{seg}
		oldBytes, err := marshalCorpus(old)
		if err != nil {
			t.Fatal(err)
		}
		oldCatalog := compressedSize(oldBytes)

		var blocks [][]byte
		var sparse []byte
		var externalOffset, blockOffset uint64
		var inlineTerms int
		for start := 0; start < len(terms); start += 512 {
			end := min(start+512, len(terms))
			sparse = appendString(sparse, terms[start])
			sparse = appendUint(sparse, blockOffset)
			var raw []byte
			previous := ""
			for _, term := range terms[start:end] {
				prefix := 0
				for prefix < min(len(previous), len(term)) && previous[prefix] == term[prefix] {
					prefix++
				}
				raw = appendUint(raw, uint64(prefix))
				raw = appendString(raw, term[prefix:])
				raw = appendUint(raw, uint64(len(lists[term])))
				if len(lists[term]) <= 6 {
					inlineTerms++
					raw = append(raw, 1)
					raw = append(raw, varintPostings(lists[term])...)
				} else {
					raw = append(raw, 0)
					raw = appendUint(raw, externalOffset)
					postingData := varintPostings(lists[term])
					size := partSize(postingData)
					raw = appendUint(raw, uint64(size))
					raw = binary.LittleEndian.AppendUint32(raw, crc32.Checksum(postingData, crcTable))
					externalOffset += uint64(size)
				}
				previous = term
			}
			var compressed bytes.Buffer
			zw := zlib.NewWriter(&compressed)
			_, _ = zw.Write(raw)
			_ = zw.Close()
			sealed := binary.LittleEndian.AppendUint32(compressed.Bytes(), crc32.Checksum(raw, crcTable))
			blocks = append(blocks, sealed)
			blockOffset += uint64(len(sealed))
		}
		sparseReader := manifestReader{data: sparse}
		var checkedOffset uint64
		for blockNo, block := range blocks {
			first, err := sparseReader.string()
			if err != nil || first != terms[blockNo*512] {
				t.Fatal("sparse dictionary key mismatch")
			}
			offset, err := sparseReader.uint()
			if err != nil || offset != checkedOffset {
				t.Fatal("sparse dictionary offset mismatch")
			}
			checkedOffset += uint64(len(block))
		}
		if sparseReader.pos != len(sparse) || checkedOffset != blockOffset {
			t.Fatal("sparse dictionary size mismatch")
		}
		// Read the actual compressed block bytes and verify every entry, not just size math.
		var decoded int
		for blockNo, compressed := range blocks {
			zr, err := zlib.NewReader(bytes.NewReader(compressed[:len(compressed)-4]))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(zr)
			_ = zr.Close()
			if err != nil {
				t.Fatal(err)
			}
			if binary.LittleEndian.Uint32(compressed[len(compressed)-4:]) != crc32.Checksum(raw, crcTable) {
				t.Fatal("dictionary block checksum mismatch")
			}
			r := manifestReader{data: raw}
			previous := ""
			for decoded < min((blockNo+1)*512, len(terms)) {
				prefix, err := r.uint()
				if err != nil || prefix > uint64(len(previous)) {
					t.Fatal("invalid dictionary prefix")
				}
				suffix, err := r.string()
				if err != nil {
					t.Fatal(err)
				}
				term := previous[:prefix] + suffix
				count, err := r.uint()
				if err != nil || term != terms[decoded] || count != uint64(len(lists[term])) {
					t.Fatal("dictionary term or DF mismatch")
				}
				flag, err := r.bytes(1)
				if err != nil {
					t.Fatal(err)
				}
				if flag[0] == 1 {
					var postings []posting
					prev := -1
					for range count {
						delta, err := r.uint()
						if err != nil {
							t.Fatal(err)
						}
						tf, err := r.uint()
						if err != nil {
							t.Fatal(err)
						}
						prev += int(delta)
						postings = append(postings, posting{id: prev, tf: int(tf)})
					}
					if !slices.Equal(postings, lists[term]) {
						t.Fatal("inline postings mismatch")
					}
				} else if flag[0] == 0 {
					if _, err := r.uint(); err != nil {
						t.Fatal(err)
					}
					postingData := varintPostings(lists[term])
					if size, err := r.uint(); err != nil || size != uint64(partSize(postingData)) {
						t.Fatal("external posting length mismatch")
					}
					checksum, err := r.bytes(4)
					if err != nil || binary.LittleEndian.Uint32(checksum) != crc32.Checksum(postingData, crcTable) {
						t.Fatal("external posting checksum mismatch")
					}
				} else {
					t.Fatal("unknown posting representation")
				}
				previous = term
				decoded++
			}
			if r.pos != len(raw) {
				t.Fatal("trailing dictionary block data")
			}
		}
		if decoded != len(terms) || externalOffset != uint64(externalBytes) {
			t.Fatal("dictionary directory mismatch")
		}
		var blockBytes int
		for _, block := range blocks {
			blockBytes += len(block)
		}
		// No source/column bytes are counted; they are identical for both choices.
		t.Logf("dictionary profile=%s terms=%d inline=%d blocks=%d oldCatalog=%d oldPostings=%d oldTotal=%d newBlocks=%d sparse=%d externalPostings=%d newTotal=%d", profile, len(terms), inlineTerms, len(blocks), oldCatalog, oldPostings, oldCatalog+oldPostings, blockBytes, len(sparse), externalBytes, blockBytes+len(sparse)+externalBytes)
	}
}
