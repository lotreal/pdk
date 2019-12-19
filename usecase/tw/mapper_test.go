package tw_test

import (
	"testing"

	"github.com/pilosa/pdk/usecase/tw"
)

func TestGetBitMappers(t *testing.T) {
	t.Log(tw.GetBitMappers("testdata/test.schema"))
}

func TestMappingRecord(t *testing.T) {
	bms := tw.GetBitMappers("testdata/test.schema")

	rec := "100,1000,200"
	record := tw.CsvRecord{Val: rec, Type: '-'}
	cts, err := tw.MappingRecord(record, bms)
	t.Log(cts)
	t.Log(err)
}
