package tw_test

import (
	"testing"

	"github.com/pilosa/pdk/usecase/tw"
)

func TestGetField(t *testing.T) {
	t.Log(tw.GetFields())
}

func TestCreateSchema(t *testing.T) {
	schema := tw.CreateSchema("dm")
	t.Log(schema)
}
