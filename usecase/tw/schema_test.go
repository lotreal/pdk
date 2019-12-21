package tw_test

import (
	"testing"

	"github.com/pilosa/pdk/usecase/tw"
)

var schema = tw.NewSchema("testdata/test.schema")

func TestCreateSchema(t *testing.T) {
	schema := tw.NewPilosaSchema("dm", schema)
	t.Log(schema)
}
