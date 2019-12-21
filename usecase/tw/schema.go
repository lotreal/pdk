package tw

import (
	"io/ioutil"
	"log"
	"strings"

	gopilosa "github.com/pilosa/go-pilosa"
)

type Schema struct {
	CsvFields map[string]int
}

func NewSchema(fileName string) Schema {
	log.Printf("This import use schema file: %s", fileName)
	fileBytes, err := ioutil.ReadFile(fileName)

	if err != nil {
		log.Fatalf("Schema file error: %v", err)
	}

	fields := strings.Split(string(fileBytes), "\n")

	fieldMap := make(map[string]int)
	for i, v := range fields {
		fieldMap[v] = i
	}

	return Schema{CsvFields: fieldMap}
}

func NewPilosaSchema(name string, schema Schema) *gopilosa.Schema {
	gpSchema := gopilosa.NewSchema()
	index := gpSchema.Index(name, gopilosa.OptIndexTrackExistence(false))

	for k, _ := range schema.CsvFields {
		index.Field(k, gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 50000))
		log.Printf("create field %s.%s", name, k)
	}

	return gpSchema
}