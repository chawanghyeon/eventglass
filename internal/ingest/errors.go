package ingest

import "errors"

var (
	ErrAdmissionLimited = errors.New("ingest byte admission limit reached")
	ErrDraining         = errors.New("ingest is draining")
	ErrProjectDisabled  = errors.New("ingest project is disabled")
	ErrKeyRevoked       = errors.New("ingest key is revoked")
	ErrScrubChanged     = errors.New("ingest scrub revision changed")
)
