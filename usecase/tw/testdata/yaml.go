package main

import (
	"fmt"
	"log"

	"gopkg.in/yaml.v2"
)

var data = `
input:
  fields:
  - name: vendor_id
    sn: 9
  - name: pickup_datetime
`

// Note: struct fields must be public in order for unmarshal to
// correctly populate the data.
type T struct {
	Input struct {
		Fields []Field
	}
}

type Field struct {
	Name string
	Sn   int
}

func main() {
	t := T{}

	err := yaml.Unmarshal([]byte(data), &t)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- t:\n%v\n\n", t)

	d, err := yaml.Marshal(&t)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- t dump:\n%s\n\n", string(d))

	m := make(map[interface{}]interface{})

	err = yaml.Unmarshal([]byte(data), &m)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- m:\n%v\n\n", m)

	d, err = yaml.Marshal(&m)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- m dump:\n%s\n\n", string(d))
}
