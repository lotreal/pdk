package tw_test

import (
	"testing"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk/usecase/tw"
	"github.com/pilosa/pilosa/test"
)

func TestRunMain(t *testing.T) {
	// start up pilosa cluster
	cluster := test.MustRunCluster(t, 3)
	client, err := gopilosa.NewClient([]string{cluster[0].URL(), cluster[1].URL(), cluster[2].URL()})
	if err != nil {
		t.Fatalf("getting new client: %v", err)
	}

	// run dm import with testdata
	main := tw.NewMain()
	main.URLFile = "testdata/urls.txt"
	main.Index = "dm"
	main.Concurrency = 2
	main.FetchConcurrency = 3
	main.PilosaHost = cluster[0].URL()

	main.BufferSize = 100000
	err = main.Run()
	if err != nil {
		t.Fatalf("running dm main: %v", err)
	}

	// query pilosa to ensure consistent results
	index, didField := GetField(t, client, "dm", "did")

	resp, err := client.Query(index.Count(didField.Row(200)))
	if err != nil {
		t.Fatalf("count querying: %v", err)
	}
	if resp.Result().Count() != 2 {
		t.Fatalf("did 200 should have 1, but got %d", resp.Result().Count())
	}

	// The cache needs to be refreshed before querying TopN.
	client.HttpRequest("POST", "/recalculate-caches", nil, nil)

	resp, err = client.Query(didField.TopN(5))
	if err != nil {
		t.Errorf("topn query: %v", err)
	}
	items := resp.Result().CountItems()
	if len(items) != 3 {
		t.Errorf("wrong number of results for Topn(did): %v", items)
	}
	if items[0].ID != 200 || items[0].Count != 2 {
		t.Errorf("wrong first item for Topn(did): %v", items)
	}

	if len(items) < 2 || items[1].ID != 2000 || items[1].Count != 1 {
		t.Errorf("wrong second item for Topn(did): %v", items)
	}
}

func GetField(t *testing.T, c *gopilosa.Client, index, field string) (*gopilosa.Index, *gopilosa.Field) {
	schema, err := c.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	idx := schema.Index(index)
	fram := idx.Field(field)
	err = c.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	return idx, fram
}
