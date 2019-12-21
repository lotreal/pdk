package tw

import (
	"github.com/pilosa/pdk"
)

var (
	trim4test = int64(65535)
	intMapper = pdk.IntMapper{Min: 0, Max: trim4test}
	dmMapper  = pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			i := fields[0].(int64)
			if i > trim4test {
				return int64(0)
			}
			return i
		},
		Mapper: intMapper,
	}
)

func GetBitMappers(schema Schema ) []pdk.ColumnMapper {
	bms := make([]pdk.ColumnMapper, len(schema.CsvFields))

	for k, v := range schema.CsvFields {
		bms[v] = pdk.ColumnMapper{
			Field:   k,
			Mapper:  dmMapper,
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{schema.CsvFields[k]},
		}

	}

	return bms
}
