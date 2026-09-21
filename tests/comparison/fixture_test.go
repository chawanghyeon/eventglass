package comparison

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"testing"
)

type fixtureSummary struct {
	Rows, Errors, Logs, Duplicates, IDLess, Late, Nulls, Missing, Dotted, NumericMatches int64
	SHA256                                                                               string
}

var fixedFixtureSummaries = map[int]fixtureSummary{
	10_000:     {Rows: 10_000, Errors: 477, Logs: 9_523, Duplicates: 94, IDLess: 589, Late: 104, Nulls: 323, Missing: 345, Dotted: 435, NumericMatches: 3_554, SHA256: "ff33e7642af23cf05701dc5d6b16bf909258d887ade68cc4846962720f744f35"},                                         // pragma: allowlist secret -- public fixture checksum
	100_000:    {Rows: 100_000, Errors: 4_762, Logs: 95_238, Duplicates: 932, IDLess: 5_883, Late: 1_031, Nulls: 3_226, Missing: 3_449, Dotted: 4_348, NumericMatches: 35_510, SHA256: "1878200018c046df95e72d6336c2f12f6a3de7a420561d083398c55d73fa341d"},                         // pragma: allowlist secret -- public fixture checksum
	1_000_000:  {Rows: 1_000_000, Errors: 47_620, Logs: 952_380, Duplicates: 9_318, IDLess: 58_824, Late: 10_310, Nulls: 32_259, Missing: 34_483, Dotted: 43_479, NumericMatches: 355_066, SHA256: "ba8b46321578366ec0cca694517e040ee73b2c9e4f67463bca55aeb1b206cd64"},             // pragma: allowlist secret -- public fixture checksum
	10_000_000: {Rows: 10_000_000, Errors: 476_191, Logs: 9_523_809, Duplicates: 93_185, IDLess: 588_236, Late: 103_093, Nulls: 322_581, Missing: 344_828, Dotted: 434_783, NumericMatches: 3_550_611, SHA256: "9326f0f738ec43593ab1dd4b18dd08bb0531294a77ef88783eeba854f6bfe375"}, // pragma: allowlist secret -- public fixture checksum
}

func TestFixedDatasetProfilesMatchIndependentOracle(t *testing.T) {
	missing := false
	for rows, expected := range fixedFixtureSummaries {
		actual := summarizeFixture(rows)
		if expected.SHA256 == "" {
			t.Logf("freeze fixture %d summary: %#v", rows, actual)
			missing = true
			continue
		}
		if actual != expected {
			t.Fatalf("fixture %d changed\nwant=%#v\n got=%#v", rows, expected, actual)
		}
		t.Logf("R3 fixture rows=%d sha256=%s matches=%d duplicates=%d idless=%d", rows, actual.SHA256, actual.NumericMatches, actual.Duplicates, actual.IDLess)
	}
	if missing {
		t.Fatal("fixed fixture summaries must be frozen")
	}
}

// summarizeFixture streams the fixture so the 10m profile stays bounded. The
// hash covers every semantic selector; it is not derived from Eventglass code.
func summarizeFixture(rows int) fixtureSummary {
	result := fixtureSummary{Rows: int64(rows)}
	digest := sha256.New()
	var encoded [24]byte
	for index := 0; index < rows; index++ {
		isError := index%21 == 0
		hasID := index%17 != 0
		duplicate := hasID && index%101 == 100
		late := index%97 == 0
		nullValue := index%31 == 0
		missing := index%29 == 0
		dotted := index%23 == 0
		mixedNumeric := index % 3
		value := int64(index%1000) - 500
		eventSecond := int64(index / 2) // equal timestamps are deliberate.
		if late {
			eventSecond -= 3600
		}
		if isError {
			result.Errors++
		} else {
			result.Logs++
		}
		if duplicate {
			result.Duplicates++
		}
		if !hasID {
			result.IDLess++
		}
		if late {
			result.Late++
		}
		if nullValue {
			result.Nulls++
		}
		if missing {
			result.Missing++
		}
		if dotted {
			result.Dotted++
		}
		// Independent oracle for: log AND present/non-null numeric > 100.
		if !isError && !missing && !nullValue && value > 100 {
			result.NumericMatches++
		}
		binary.LittleEndian.PutUint64(encoded[0:8], uint64(index))
		binary.LittleEndian.PutUint64(encoded[8:16], uint64(eventSecond))
		binary.LittleEndian.PutUint64(encoded[16:24], uint64(value))
		_, _ = digest.Write(encoded[:])
		writeFixtureFlags(digest, isError, hasID, duplicate, late, nullValue, missing, dotted, mixedNumeric)
	}
	result.SHA256 = fmt.Sprintf("%x", digest.Sum(nil))
	return result
}

func writeFixtureFlags(digest hash.Hash, flags ...any) {
	for _, flag := range flags {
		switch value := flag.(type) {
		case bool:
			if value {
				_, _ = digest.Write([]byte{1})
			} else {
				_, _ = digest.Write([]byte{0})
			}
		case int:
			_, _ = digest.Write([]byte{byte(value)})
		}
	}
}
