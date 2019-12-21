package tw

import (
	"github.com/pilosa/pdk"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	gopilosa "github.com/pilosa/go-pilosa"
)

type SchemaConfig struct {
	CsvFields map[string]int
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

	return SchemaConfig{CsvFields: fieldMap}
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
	Type rune
	Val  string
}

func (r CsvRecord) clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

func InsertRecord(indexer pdk.Indexer, record CsvRecord, schema SchemaConfig) {
	records, _ := record.clean()
	// log.Printf("DM.id=%s", records[1])
	columnID, err := strconv.ParseUint(records[1], 10, 64)
	if err != nil {
		log.Printf("ColumnID parse (%s) return error: %v", records[1], err)
	}

	for name, idx := range schema.CsvFields {
		row, err2 := strconv.ParseInt(records[idx], 10, 64)
		if err2 != nil {
			log.Printf("Field parse (%s) return error: %v", records[idx], err)
		}
		// log.Printf("DM.AddColumn(%s, %d, %d)", name, columnID, row)
		indexer.AddColumn(name, uint64(columnID), uint64(row))
	}
}
