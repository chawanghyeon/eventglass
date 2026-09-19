package query

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestReadTokenRoundTripRotationAndErrorPrecedence(t *testing.T) {
	current := SigningKey{ID: "current", Secret: sha256.Sum256([]byte("current signing key"))}
	previous := SigningKey{ID: "previous", Secret: sha256.Sum256([]byte("previous signing key"))}
	codec, err := NewTokenCodec(current, &previous)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	codec.now = func() time.Time { return now }
	principal := strings.Repeat("a", 64)
	dataset := strings.Repeat("b", 64)
	snapshot := model.QuerySnapshot{StorageGeneration: 7, SnapshotID: "00000000-0000-4000-8000-000000000007", PrincipalHash: principal, DatasetSHA256: dataset, ExpiresAtUS: now.Add(15 * time.Minute).UnixMicro()}
	token, err := codec.SignRead(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	expected := TokenExpectation{Generation: 7, PrincipalHash: principal, DatasetHash: dataset}
	got, err := codec.VerifyRead(token, expected)
	if err != nil || got.SnapshotID != snapshot.SnapshotID || got.KeyID != "current" {
		t.Fatalf("claims=%#v err=%v", got, err)
	}

	oldCodec, _ := NewTokenCodec(previous, nil)
	oldCodec.now = codec.now
	oldToken, _ := oldCodec.SignRead(snapshot)
	if _, err := codec.VerifyRead(oldToken, expected); err != nil {
		t.Fatalf("previous key rejected: %v", err)
	}

	parts := strings.Split(token, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[0])
	payload[0] ^= 1
	tampered := base64.RawURLEncoding.EncodeToString(payload) + "." + parts[1]
	if _, err := codec.VerifyRead(tampered, TokenExpectation{Generation: 8, PrincipalHash: "wrong", DatasetHash: "wrong"}); !errors.Is(err, ErrTokenMalformed) {
		t.Fatalf("MAC did not take precedence: %v", err)
	}
	if _, err := codec.VerifyRead(token, TokenExpectation{Generation: 8, PrincipalHash: "wrong", DatasetHash: "wrong"}); !errors.Is(err, ErrTokenGenerationChanged) {
		t.Fatalf("generation precedence=%v", err)
	}
	if _, err := codec.VerifyRead(token, TokenExpectation{Generation: 7, PrincipalHash: strings.Repeat("c", 64), DatasetHash: "wrong"}); !errors.Is(err, ErrTokenForbidden) {
		t.Fatalf("authority precedence=%v", err)
	}
	codec.now = func() time.Time { return now.Add(16 * time.Minute) }
	if _, err := codec.VerifyRead(token, TokenExpectation{Generation: 7, PrincipalHash: principal, DatasetHash: "wrong"}); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expiry precedence=%v", err)
	}
	codec.now = func() time.Time { return now }
	if _, err := codec.VerifyRead(token, TokenExpectation{Generation: 7, PrincipalHash: principal, DatasetHash: "wrong"}); !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("dataset mismatch=%v", err)
	}
}

func TestCursorBindsOperationSortLimitAndCompleteTieTuple(t *testing.T) {
	codec, _ := NewTokenCodec(SigningKey{ID: "one", Secret: sha256.Sum256([]byte("cursor signing key"))}, nil)
	now := time.Unix(1_800_000_000, 0)
	codec.now = func() time.Time { return now }
	eventUS, eventNS := int64(42), 999
	snapshot := model.QuerySnapshot{StorageGeneration: 3, SnapshotID: "00000000-0000-4000-8000-000000000003", PrincipalHash: strings.Repeat("a", 64), DatasetSHA256: strings.Repeat("b", 64), ExpiresAtUS: now.Add(time.Minute).UnixMicro()}
	operationHash, sortName, limit := strings.Repeat("c", 64), "event_desc", 100
	last := CursorTuple{EventUS: &eventUS, EventNS: &eventNS, RecordID: strings.Repeat("d", 64)}
	token, err := codec.SignCursor(snapshot, operationHash, sortName, limit, last)
	if err != nil {
		t.Fatal(err)
	}
	expected := TokenExpectation{Generation: 3, PrincipalHash: snapshot.PrincipalHash, DatasetHash: snapshot.DatasetSHA256, OperationHash: operationHash, Sort: sortName, Limit: limit}
	got, err := codec.VerifyCursor(token, expected)
	if err != nil || got.Last.EventUS == nil || *got.Last.EventUS != eventUS || got.Last.RecordID != last.RecordID {
		t.Fatalf("claims=%#v err=%v", got, err)
	}
	expected.Limit++
	if _, err := codec.VerifyCursor(token, expected); !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("changed limit accepted: %v", err)
	}

	receivedUS, lane, batch, ordinal := int64(42), 15, int64(9), 0
	sortName, last = "received_desc", CursorTuple{ReceivedUS: &receivedUS, LaneID: &lane, BatchSeq: &batch, RecordOrdinal: &ordinal, RecordID: last.RecordID}
	if _, err := codec.SignCursor(snapshot, operationHash, sortName, limit, last); err != nil {
		t.Fatalf("received tie tuple rejected: %v", err)
	}
	last.EventUS = &eventUS
	if _, err := codec.SignCursor(snapshot, operationHash, sortName, limit, last); !errors.Is(err, ErrTokenMismatch) {
		t.Fatalf("mixed cursor tuple accepted: %v", err)
	}
}

func TestCursorPredicatesBindEveryTieBreaker(t *testing.T) {
	eventUS, eventNS := int64(42), 7
	event, err := CompileCursorPredicate("event_desc", CursorTuple{EventUS: &eventUS, EventNS: &eventNS, RecordID: strings.Repeat("b", 64)})
	if err != nil || event.Text != `(r.event_time_us,r.event_time_ns_remainder,r.record_id) < (?,?,?)` || len(event.Args) != 3 {
		t.Fatalf("event cursor=%#v err=%v", event, err)
	}
	receivedUS, lane, batch, ordinal := int64(43), 2, int64(9), 4
	received, err := CompileCursorPredicate("received_desc", CursorTuple{ReceivedUS: &receivedUS, LaneID: &lane, BatchSeq: &batch, RecordOrdinal: &ordinal, RecordID: strings.Repeat("c", 64)})
	if err != nil || received.Text != `(r.received_time_us,r.lane_id,r.batch_seq,r.record_ordinal,r.record_id) < (?,?,?,?,?)` || len(received.Args) != 5 {
		t.Fatalf("received cursor=%#v err=%v", received, err)
	}
}

func TestTokenRejectsNonCanonicalOrOversizedPayload(t *testing.T) {
	codec, _ := NewTokenCodec(SigningKey{ID: "one", Secret: sha256.Sum256([]byte("canonical signing key"))}, nil)
	codec.now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	snapshot := model.QuerySnapshot{StorageGeneration: 1, SnapshotID: "00000000-0000-4000-8000-000000000001", PrincipalHash: strings.Repeat("a", 64), DatasetSHA256: strings.Repeat("b", 64), ExpiresAtUS: codec.now().Add(time.Minute).UnixMicro()}
	token, _ := codec.SignRead(snapshot)
	parts := strings.Split(token, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[0])
	spaced := append([]byte(" "), payload...)
	mac := hmac.New(sha256.New, codec.current.Secret[:])
	mac.Write(spaced)
	noncanonical := base64.RawURLEncoding.EncodeToString(spaced) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if _, err := codec.VerifyRead(noncanonical, TokenExpectation{}); !errors.Is(err, ErrTokenMalformed) {
		t.Fatalf("noncanonical payload accepted: %v", err)
	}
	oversized := base64.RawURLEncoding.EncodeToString(make([]byte, MaxTokenPayload+1)) + "." + parts[1]
	if _, err := codec.VerifyRead(oversized, TokenExpectation{}); !errors.Is(err, ErrTokenMalformed) {
		t.Fatalf("oversized payload accepted: %v", err)
	}
}
