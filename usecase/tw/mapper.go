package tw

import (
	"github.com/pilosa/pdk"
)

func GetBitMappers() []pdk.ColumnMapper {
	schema := GetSchema()
	fields := GetFields()

	bms := make([]pdk.ColumnMapper, len(schema))

	for i, v := range schema {
		bms[i] = pdk.ColumnMapper{
			Field:   v,
			Mapper:  pdk.IntMapper{Min: 0, Max: 10000},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{fields[v]},
		}

	}

	return bms
}
