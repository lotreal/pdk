package tw

import (
	"io/ioutil"
	"log"
	"strings"

	gopilosa "github.com/pilosa/go-pilosa"
)

type SchemaConfig struct {
	CsvFields map[string]int
	CsvFieldsNum int
}

func NewSchemaConfig(fileName string) SchemaConfig {
	log.Printf("This import use config file: %s", fileName)
	fileBytes, err := ioutil.ReadFile(fileName)

	if err != nil {
		log.Fatalf("SchemaConfig file error: %v", err)
	}

	fields := strings.Split(string(fileBytes), "\n")

	fieldMap := make(map[string]int)
	for i, v := range fields {
		fieldMap[v] = i
	}

	return SchemaConfig{CsvFields: fieldMap, CsvFieldsNum:len(fields)}
}

func NewPilosaSchema(name string, schema SchemaConfig) *gopilosa.Schema {
	gpSchema := gopilosa.NewSchema()
	index := gpSchema.Index(name, gopilosa.OptIndexTrackExistence(false))

	for k := range schema.CsvFields {
		index.Field(k, gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 50000))
		log.Printf("Set pilosa field: %s.%s", name, k)
	}

	return gpSchema
}

type CsvRecord struct {
	Val  string
}

func (r CsvRecord) clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}
