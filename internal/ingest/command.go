package ingest

import "github.com/chawanghyeon/eventglass/internal/model"

// Authorization is the project/key snapshot used for normalization. Accept
// must lock and revalidate every revision; this value is never journal data.
type Authorization struct {
	TenantID        int64
	ProjectID       int64
	KeyHash         [32]byte
	TenantRevision  int64
	ProjectRevision int64
	KeyRevision     int64
	ScrubRevision   int
	ConfigRevision  int64
}

type Command struct {
	Request       model.NormalizedRequest
	Authorization Authorization
}
