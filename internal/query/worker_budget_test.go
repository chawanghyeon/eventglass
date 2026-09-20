package query

import (
	"errors"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestQueryDiskReservationStreamsScanInputsFromSharedCache(t *testing.T) {
	manifest := TaskManifest{Files: []model.CatalogFile{{Bytes: 10, PayloadBytes: 20}}}
	got, err := queryDiskReservation(control.QueryTask{}, manifest, engine.QueryOperation{Kind: "detail"})
	if err != nil || got != engine.DefaultNativeSpillBytes+engine.MaxQueryOutputBytes {
		t.Fatalf("reservation=%d err=%v", got, err)
	}
	manifest.Files[0].Bytes = -1
	if _, err := queryDiskReservation(control.QueryTask{}, manifest, engine.QueryOperation{Kind: "rows"}); !errors.Is(err, engine.ErrQueryExecutionInvalid) {
		t.Fatal(err)
	}
}
