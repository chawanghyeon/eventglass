package query

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
)

const (
	TokenVersion       = 1
	MaxTokenPayload    = 8 << 10
	MaxTokenLifetime   = time.Hour
	ReadTokenPurpose   = "read"
	CursorTokenPurpose = "cursor"
	LiveTokenPurpose   = "live"
)

var (
	ErrTokenMalformed         = errors.New("token is malformed")
	ErrTokenGenerationChanged = errors.New("token generation changed")
	ErrTokenForbidden         = errors.New("token principal is forbidden")
	ErrTokenExpired           = errors.New("token expired")
	ErrTokenMismatch          = errors.New("token scope does not match")
)

type SigningKey struct {
	ID     string
	Secret [32]byte
}

type TokenCodec struct {
	current  SigningKey
	previous *SigningKey
	now      func() time.Time
}

type ReadTokenClaims struct {
	Version       int    `json:"version"`
	Purpose       string `json:"purpose"`
	KeyID         string `json:"key_id"`
	Generation    int64  `json:"generation"`
	SnapshotID    string `json:"snapshot_id"`
	PrincipalHash string `json:"principal_hash"`
	DatasetHash   string `json:"dataset_hash"`
	ExpiresAtUS   int64  `json:"expires_at_us"`
}

type CursorTuple struct {
	EventUS       *int64 `json:"event_us"`
	EventNS       *int   `json:"event_ns"`
	ReceivedUS    *int64 `json:"received_us"`
	LaneID        *int   `json:"lane_id"`
	BatchSeq      *int64 `json:"batch_seq"`
	RecordOrdinal *int   `json:"record_ordinal"`
	RecordID      string `json:"record_id"`
}

type CursorClaims struct {
	Version       int         `json:"version"`
	Purpose       string      `json:"purpose"`
	KeyID         string      `json:"key_id"`
	Generation    int64       `json:"generation"`
	SnapshotID    string      `json:"snapshot_id"`
	PrincipalHash string      `json:"principal_hash"`
	DatasetHash   string      `json:"dataset_hash"`
	OperationHash string      `json:"operation_hash"`
	Sort          string      `json:"sort"`
	Limit         int         `json:"limit"`
	Last          CursorTuple `json:"last"`
	ExpiresAtUS   int64       `json:"expires_at_us"`
}

type LivePosition struct {
	BatchSeq int64 `json:"batch_seq"`
	Ordinal  int   `json:"ordinal"`
}

type LiveTokenClaims struct {
	StartUS       *int64                        `json:"start_us,omitempty"`
	Version       int                           `json:"version"`
	Purpose       string                        `json:"purpose"`
	KeyID         string                        `json:"key_id"`
	Generation    int64                         `json:"generation"`
	PrincipalHash string                        `json:"principal_hash"`
	ScopeHash     string                        `json:"scope_hash"`
	Positions     [model.LaneCount]LivePosition `json:"positions"`
	ExpiresAtUS   int64                         `json:"expires_at_us"`
}

type TokenExpectation struct {
	Generation    int64
	PrincipalHash string
	DatasetHash   string
	OperationHash string
	Sort          string
	Limit         int
}

func NewTokenCodec(current SigningKey, previous *SigningKey) (*TokenCodec, error) {
	if !validSigningKey(current) || previous != nil && (!validSigningKey(*previous) || previous.ID == current.ID) {
		return nil, errors.New("invalid token signing keys")
	}
	copyPrevious := previous
	if previous != nil {
		value := *previous
		copyPrevious = &value
	}
	return &TokenCodec{current: current, previous: copyPrevious, now: time.Now}, nil
}

func PrincipalHash(userID int64, sessionTokenHash [32]byte) string {
	return model.QueryPrincipalHash(userID, sessionTokenHash)
}

func (codec *TokenCodec) SignRead(snapshot model.QuerySnapshot) (string, error) {
	claims := ReadTokenClaims{
		Version: TokenVersion, Purpose: ReadTokenPurpose, KeyID: codec.current.ID,
		Generation: snapshot.StorageGeneration, SnapshotID: snapshot.SnapshotID, PrincipalHash: snapshot.PrincipalHash,
		DatasetHash: snapshot.DatasetSHA256, ExpiresAtUS: snapshot.ExpiresAtUS,
	}
	if err := codec.validateCommon(claims.Generation, claims.SnapshotID, claims.PrincipalHash, claims.DatasetHash, claims.ExpiresAtUS); err != nil {
		return "", err
	}
	return codec.sign(claims)
}

func (codec *TokenCodec) SignCursor(snapshot model.QuerySnapshot, operationHash, sortName string, limit int, last CursorTuple) (string, error) {
	claims := CursorClaims{
		Version: TokenVersion, Purpose: CursorTokenPurpose, KeyID: codec.current.ID,
		Generation: snapshot.StorageGeneration, SnapshotID: snapshot.SnapshotID, PrincipalHash: snapshot.PrincipalHash,
		DatasetHash: snapshot.DatasetSHA256, OperationHash: operationHash, Sort: sortName, Limit: limit, Last: last,
		ExpiresAtUS: snapshot.ExpiresAtUS,
	}
	if err := codec.validateCommon(claims.Generation, claims.SnapshotID, claims.PrincipalHash, claims.DatasetHash, claims.ExpiresAtUS); err != nil {
		return "", err
	}
	if !validDigest(claims.OperationHash) || !validCursor(claims.Sort, claims.Limit, claims.Last) {
		return "", ErrTokenMismatch
	}
	return codec.sign(claims)
}

func (codec *TokenCodec) VerifyRead(token string, expected TokenExpectation) (ReadTokenClaims, error) {
	var claims ReadTokenClaims
	if err := codec.verify(token, ReadTokenPurpose, &claims); err != nil {
		return ReadTokenClaims{}, err
	}
	if claims.Generation <= 0 || uuid.Validate(claims.SnapshotID) != nil || !validDigest(claims.PrincipalHash) || !validDigest(claims.DatasetHash) {
		return ReadTokenClaims{}, ErrTokenMalformed
	}
	if err := codec.checkExpected(claims.Generation, claims.PrincipalHash, claims.DatasetHash, "", "", 0, claims.ExpiresAtUS, expected); err != nil {
		return ReadTokenClaims{}, err
	}
	return claims, nil
}

func (codec *TokenCodec) VerifyCursor(token string, expected TokenExpectation) (CursorClaims, error) {
	var claims CursorClaims
	if err := codec.verify(token, CursorTokenPurpose, &claims); err != nil {
		return CursorClaims{}, err
	}
	if claims.Generation <= 0 || uuid.Validate(claims.SnapshotID) != nil || !validDigest(claims.PrincipalHash) || !validDigest(claims.DatasetHash) || !validDigest(claims.OperationHash) || !validCursor(claims.Sort, claims.Limit, claims.Last) {
		return CursorClaims{}, ErrTokenMalformed
	}
	if err := codec.checkExpected(claims.Generation, claims.PrincipalHash, claims.DatasetHash, claims.OperationHash, claims.Sort, claims.Limit, claims.ExpiresAtUS, expected); err != nil {
		return CursorClaims{}, err
	}
	return claims, nil
}

func (codec *TokenCodec) SignLive(generation int64, principalHash, scopeHash string, positions [model.LaneCount]LivePosition, expiresAtUS int64) (string, error) {
	return codec.SignLiveFrom(generation, principalHash, scopeHash, positions, expiresAtUS, math.MinInt64)
}

func (codec *TokenCodec) SignLiveFrom(generation int64, principalHash, scopeHash string, positions [model.LaneCount]LivePosition, expiresAtUS, startUS int64) (string, error) {
	claims := LiveTokenClaims{
		StartUS: &startUS,
		Version: TokenVersion, Purpose: LiveTokenPurpose, KeyID: codec.current.ID,
		Generation: generation, PrincipalHash: principalHash, ScopeHash: scopeHash,
		Positions: positions, ExpiresAtUS: expiresAtUS,
	}
	if !validLiveClaims(claims, codec.now()) {
		return "", ErrTokenMismatch
	}
	return codec.sign(claims)
}

func (codec *TokenCodec) VerifyLive(token string, expected TokenExpectation, scopeHash string) (LiveTokenClaims, error) {
	var claims LiveTokenClaims
	if err := codec.verify(token, LiveTokenPurpose, &claims); err != nil {
		return LiveTokenClaims{}, err
	}
	if !validLiveClaims(claims, time.Time{}) {
		return LiveTokenClaims{}, ErrTokenMalformed
	}
	if err := codec.checkExpected(claims.Generation, claims.PrincipalHash, "", "", "", 0, claims.ExpiresAtUS, expected); err != nil {
		return LiveTokenClaims{}, err
	}
	if claims.ScopeHash != scopeHash {
		return LiveTokenClaims{}, ErrTokenMismatch
	}
	return claims, nil
}

func (codec *TokenCodec) sign(claims any) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil || len(payload) > MaxTokenPayload {
		return "", ErrTokenMalformed
	}
	mac := hmac.New(sha256.New, codec.current.Secret[:])
	mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (codec *TokenCodec) verify(token, purpose string, target any) error {
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ErrTokenMalformed
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) == 0 || len(payload) > MaxTokenPayload {
		return ErrTokenMalformed
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(signature) != sha256.Size {
		return ErrTokenMalformed
	}
	var header struct {
		Version int    `json:"version"`
		Purpose string `json:"purpose"`
		KeyID   string `json:"key_id"`
	}
	// This first parse selects a key from untrusted bytes. No claim is used until
	// the MAC succeeds and the complete payload is decoded canonically below.
	if err := json.Unmarshal(payload, &header); err != nil {
		return ErrTokenMalformed
	}
	key, ok := codec.key(header.KeyID)
	if !ok {
		return ErrTokenMalformed
	}
	mac := hmac.New(sha256.New, key.Secret[:])
	mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return ErrTokenMalformed
	}
	if header.Version != TokenVersion || header.Purpose != purpose || decodeExact(payload, target) != nil {
		return ErrTokenMalformed
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ErrTokenMalformed
	}
	return nil
}

func decodeExact(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrTokenMalformed
	}
	return nil
}

func (codec *TokenCodec) validateCommon(generation int64, snapshotID, principalHash, datasetHash string, expiresAtUS int64) error {
	now := codec.now()
	expires := time.UnixMicro(expiresAtUS)
	if generation <= 0 || uuid.Validate(snapshotID) != nil || !validDigest(principalHash) || !validDigest(datasetHash) || !expires.After(now) || expires.After(now.Add(MaxTokenLifetime)) {
		return ErrTokenMismatch
	}
	return nil
}

func (codec *TokenCodec) checkExpected(generation int64, principalHash, datasetHash, operationHash, sortName string, limit int, expiresAtUS int64, expected TokenExpectation) error {
	// Error precedence is public contract: authenticated syntax/MAC first (done
	// by verify), then generation, current authority, expiry and scope mismatch.
	if generation != expected.Generation {
		return ErrTokenGenerationChanged
	}
	if !hmac.Equal([]byte(principalHash), []byte(expected.PrincipalHash)) {
		return ErrTokenForbidden
	}
	if !time.UnixMicro(expiresAtUS).After(codec.now()) {
		return ErrTokenExpired
	}
	if expected.DatasetHash != "" && datasetHash != expected.DatasetHash || expected.OperationHash != operationHash || expected.Sort != sortName || expected.Limit != limit {
		return ErrTokenMismatch
	}
	return nil
}

func (codec *TokenCodec) key(id string) (SigningKey, bool) {
	if id == codec.current.ID {
		return codec.current, true
	}
	if codec.previous != nil && id == codec.previous.ID {
		return *codec.previous, true
	}
	return SigningKey{}, false
}

func validSigningKey(key SigningKey) bool {
	return key.ID != "" && len(key.ID) <= 64 && key.Secret != ([32]byte{})
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func validCursor(sortName string, limit int, last CursorTuple) bool {
	if limit < 1 || limit > 1000 || !validDigest(last.RecordID) {
		return false
	}
	switch sortName {
	case "event_desc":
		return last.EventUS != nil && last.EventNS != nil && *last.EventNS >= 0 && *last.EventNS <= 999 && last.ReceivedUS == nil && last.LaneID == nil && last.BatchSeq == nil && last.RecordOrdinal == nil
	case "received_desc":
		return last.EventUS == nil && last.EventNS == nil && last.ReceivedUS != nil && last.LaneID != nil && *last.LaneID >= 0 && *last.LaneID < 16 && last.BatchSeq != nil && *last.BatchSeq > 0 && last.RecordOrdinal != nil && *last.RecordOrdinal >= 0
	default:
		return false
	}
}

func validLiveClaims(claims LiveTokenClaims, now time.Time) bool {
	if claims.Version != TokenVersion || claims.Purpose != LiveTokenPurpose || claims.Generation <= 0 || !validDigest(claims.PrincipalHash) || !validDigest(claims.ScopeHash) {
		return false
	}
	for _, position := range claims.Positions {
		if position.BatchSeq < 0 || position.Ordinal < -1 || position.BatchSeq == 0 && position.Ordinal != -1 {
			return false
		}
	}
	if !now.IsZero() {
		expires := time.UnixMicro(claims.ExpiresAtUS)
		if !expires.After(now) || expires.After(now.Add(MaxTokenLifetime)) {
			return false
		}
	}
	return true
}
