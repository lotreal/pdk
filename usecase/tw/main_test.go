// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package tw_test

import (
	"fmt"
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

	// run taxi import with testdata
	main := tw.NewMain()
	main.URLFile = "testdata/urls.txt"
	main.Index = "taxi"
	main.Concurrency = 2
	main.FetchConcurrency = 3
	main.PilosaHost = cluster[0].URL()

	main.BufferSize = 100000
	err = main.Run()
	if err != nil {
		t.Fatalf("running taxi main: %v", err)
	}

	// query pilosa to ensure consistent results
	index, cabTypeField := GetField(t, client, "taxi", "cab_type")

	resp, err := client.Query(index.Count(cabTypeField.Row(0)))
	if err != nil {
		t.Fatalf("count querying: %v", err)
	}
	if resp.Result().Count() != 34221 {
		t.Fatalf("cab_type 0 should have 34221, but got %d", resp.Result().Count())
	}

	resp, err = client.Query(index.Count(cabTypeField.Row(1)))
	if err != nil {
		t.Fatalf("count querying: %v", err)
	}
	if resp.Result().Count() != 87793 {
		t.Errorf("cab_type 0 should have 87793, but got %d", resp.Result().Count())
	}

	// The cache needs to be refreshed before querying TopN.
	client.HttpRequest("POST", "/recalculate-caches", nil, nil)

	resp, err = client.Query(cabTypeField.TopN(5))
	if err != nil {
		t.Errorf("topn query: %v", err)
	}
	items := resp.Result().CountItems()
	if len(items) != 2 {
		t.Errorf("wrong number of results for Topn(cab_type): %v", items)
	}
	if items[0].ID != 1 || items[0].Count != 87793 {
		t.Errorf("wrong first item for Topn(cab_type): %v", items)
	}

	if len(items) < 2 || items[1].ID != 0 || items[1].Count != 34221 {
		t.Errorf("wrong second item for Topn(cab_type): %v", items)
	}

	_, centsField := GetField(t, client, "taxi", "cost_cents")

	resp, err = client.Query(centsField.Sum(cabTypeField.Row(0)))
	if err != nil {
		t.Fatalf("querying for cents sum: %v", err)
	}
	val := resp.ResultList[0].Value()
	if val != 24070186 {
		t.Errorf("%d is wrong for sum of cost_cents for green", val)
	}

	resp, err = client.Query(centsField.Sum(cabTypeField.Row(1)))
	if err != nil {
		t.Fatalf("querying for cents sum: %v", err)
	}
	val = resp.ResultList[0].Value()
	if val != 120887728 {
		t.Errorf("%d is wrong for sum of cost_cents for yellow", val)
	}

	resp, err = client.Query(index.RawQuery("Sum(field=cost_cents)"))
	if err != nil {
		t.Fatalf("raw querying: %v", err)
	}
	if resp.ResultList[0].Value() != 144957914 {
		t.Errorf("%d is wrong for total sum of cost_cents", resp.ResultList[0].Value())
	}

	_, userField := GetField(t, client, "taxi", "user_id")
	resp, err = client.Query(index.Count(userField.Row(0)))
	if err != nil {
		t.Fatalf("count user 0 query: %v", err)
	}
	fmt.Println("user0: ", resp.Result().Count())

	resp, err = client.Query(index.Count(userField.Row(1)))
	if err != nil {
		t.Fatalf("count user 1 query: %v", err)
	}
	fmt.Println("user1: ", resp.Result().Count())

	resp, err = client.Query(userField.TopN(10))
	if err != nil {
		t.Fatalf("count user 1 query: %v", err)
	}
	fmt.Println("usertopn: ", resp.Result().CountItems())
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
