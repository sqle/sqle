package metadata

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/memory"
	"gopkg.in/sqle/sqle.v0/sql"
)

var metadataSchemata = []metadataColumn{
	{&sql.Column{Name: "catalog_name", Type: sql.String, Default: nil, Nullable: false}, "def"},
	{&sql.Column{Name: "schema_name", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "default_character_set_name", Type: sql.String, Default: "utf8", Nullable: false}, "nil"},
	{&sql.Column{Name: "default_collation_name", Type: sql.String, Default: "utf8_general_ci", Nullable: false}, "nil"},
	{&sql.Column{Name: "sql_path", Type: sql.String, Default: nil, Nullable: true}, "nil"},
}

type schemataTable struct {
	*metadataTable
	index map[string]int
}

func newSchemataTable(catalog sql.DBStorer) *schemataTable {
	schema, index := schema(metadataSchemata)
	data := schemataData{data: catalog, index: index}
	return &schemataTable{
		newTable(SchemaDBTableName, schema, data),
		index,
	}
}

func (t *schemataTable) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: %s is a table view; Insertion is not allowed", t.Name())
}

type schemataData struct {
	data  sql.DBStorer
	index map[string]int
}

func (c schemataData) IterData() memory.IteratorData {
	return &schemataIter{data: c.data.Dbs(), index: c.index}
}

func (c schemataData) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type schemataIter struct {
	data  []sql.Database
	index map[string]int
}

func (i *schemataIter) Length() int {
	return len(i.data)
}

func (i *schemataIter) Get(idx int) []interface{} {
	row := make([]interface{}, len(metadataSchemata))
	k := 0
	for _, f := range metadataSchemata {
		row[k] = i.getColumn(f.Name, i.data[idx])
		k++
	}

	return row
}

func (i *schemataIter) getColumn(name string, value sql.Database) interface{} {
	switch name {
	case "schema_name":
		return value.Name()
	default:
		return metadataSchemata[i.index[name]].def
	}
}
