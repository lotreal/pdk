package tw

import (
	"io/ioutil"
	"log"
	"os"
	"strings"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
)

func GetSchema() []string {
	fileName := "csv.schema"
	fileBytes, err := ioutil.ReadFile(fileName)

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	return strings.Split(string(fileBytes), "\n")
}

func CreateSchema(name string) *gopilosa.Schema {
	schema := gopilosa.NewSchema()
	index := schema.Index(name, gopilosa.OptIndexTrackExistence(false))

	for _, v := range GetSchema() {
		pdk.NewRankedField(index, v, 10000)
		log.Printf("create field %s.%s", name, v)
	}

	return schema
}
