package tw

import (
	"io/ioutil"
	"log"
	"os"
	"strings"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
)

func GetSchema(fileName string) []string {
	// fileName := "test.schema"
	log.Printf("Import use schema file: %s", fileName)

	fileBytes, err := ioutil.ReadFile(fileName)

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	return strings.Split(string(fileBytes), "\n")
}

func CreateSchema(name string, schemaFile string) *gopilosa.Schema {
	schema := gopilosa.NewSchema()
	index := schema.Index(name, gopilosa.OptIndexTrackExistence(false))

	for _, v := range GetSchema(schemaFile) {
		// pdk.NewIntField(index, v, 0, 65535)
		pdk.NewRankedField(index, v, 10000)
		log.Printf("create field %s.%s", name, v)
	}

	return schema
}

func GetFields(schemaFile string) map[string]int {
	fields := make(map[string]int)
	for i, v := range GetSchema(schemaFile) {
		fields[v] = i
	}
	return fields
}
