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
				return 0
			}
			return i
		},
		Mapper: intMapper,
	}
)

func GetBitMappers() []pdk.ColumnMapper {
	schema := GetSchema()
	fields := GetFields()

	bms := make([]pdk.ColumnMapper, len(schema))

	for i, v := range schema {
		bms[i] = pdk.ColumnMapper{
			Field:   v,
			Mapper:  dmMapper,
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{fields[v]},
		}

	}

	return bms
}
