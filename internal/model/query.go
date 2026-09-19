package model

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

type QueryTimeBasis string

const (
	QueryTimeEvent    QueryTimeBasis = "event"
	QueryTimeReceived QueryTimeBasis = "received"
)

type DatasetSpec struct {
	TenantID   int64
	ProjectIDs []int64
	Kinds      []Kind
	TimeBasis  QueryTimeBasis
	StartUS    int64
	EndUS      int64
	Filter     []byte
}

type QueryPlan struct {
	Version       int
	DatasetSHA256 string
	Dataset       DatasetSpec
	Snapshot      SnapshotScope
}

type SnapshotScope struct {
	RetentionFloorUS int64
	LaneCuts         [LaneCount]int64
}

type SnapshotLane struct {
	LaneID            int
	CutSeq            int64
	CatalogGeneration int64
}

type QuerySnapshot struct {
	SnapshotID         string
	TenantID           int64
	UserID             int64
	PrincipalHash      string
	AuthRevision       int64
	TenantAuthRevision int64
	StorageGeneration  int64
	DatasetSHA256      string
	DatasetBytes       []byte
	RetentionFloorUS   int64
	ProjectIDs         []int64
	ProjectRevisions   []int64
	Lanes              [LaneCount]SnapshotLane
	ExpiresAtUS        int64
	MaxUntilUS         int64
}

type CatalogFile struct {
	FileID            string
	BundleID          string
	ObjectKey         string
	Bytes             int64
	SHA256            string
	RowCount          int64
	MinEventTimeUS    int64
	MaxEventTimeUS    int64
	MinReceivedTimeUS int64
	MaxReceivedTimeUS int64
	MinBatchSeq       int64
	MaxBatchSeq       int64
	LaneID            int
	Kind              Kind
	BlockSHA256       []string
}

func QueryPrincipalHash(userID int64, sessionTokenHash [32]byte) string {
	hash := sha256.New()
	hash.Write([]byte("eventglass-principal-v1"))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(userID))
	hash.Write(encoded[:])
	hash.Write(sessionTokenHash[:])
	return hex.EncodeToString(hash.Sum(nil))
}
