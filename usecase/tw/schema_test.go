package tw_test

import (
	"testing"

	"github.com/pilosa/pdk/usecase/tw"
)

func TestGetField(t *testing.T) {
	t.Log(tw.GetFields("testdata/test.schema"))
}

func TestCreateSchema(t *testing.T) {
	schema := tw.CreateSchema("dm", "testdata/test.schema")
	t.Log(schema)
}
