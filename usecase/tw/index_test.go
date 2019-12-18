package tw_test

import (
	"testing"

	"github.com/pilosa/pdk/usecase/tw"
)

func TestIndex(t *testing.T) {
	t.Log("Hi")
	schema := tw.CreateSchema("dm")
	t.Log(schema)
}
