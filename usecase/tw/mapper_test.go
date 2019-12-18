package tw_test

import (
	"testing"

	"github.com/pilosa/pdk/usecase/tw"
)

// func TestGetBitMappers(t *testing.T) {
//	t.Log(tw.GetBitMappers())
// }

func TestMappingRecord(t *testing.T) {
	rec := "1001,1000,100,300,10,1576,1238000,100,100,86"
	record := tw.CsvRecord{Val: rec, Type: '-'}
	tw.MappingRecord(record)
}
