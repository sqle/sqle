package metadata

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/memory"
	"gopkg.in/sqle/sqle.v0/sql"
)

const (
	//SchemaDBname is the name of the sql.Table used to store catalog metadata
	SchemaDBname = "INFORMATION_SCHEMA"

	//SchemaDBTableName is the name of the Databases metadata table
	SchemaDBTableName = "SCHEMATA"

	//SchemaTableTableName is the name of the Tables metadata table
	SchemaTableTableName = "TABLES"

	//SchemaColumnTableName is the name of the Columns metadata table
	SchemaColumnTableName = "COLUMNS"
)

type metadataDB struct {
	memory.Database
	catalog sql.DBStorer
}

func NewDB(catalog sql.DBStorer) sql.Database {
	embeddedDB := memory.NewDatabase(SchemaDBname)
	m := &metadataDB{
		Database: *embeddedDB,
		catalog:  catalog,
	}

	m.addTable(newSchemataTable(catalog))
	m.addTable(newTablesTable(catalog))
	m.addTable(newcolumnsTable(catalog))
	return m
}

func (d metadataDB) AddTable(t *metadataTable) {
	panic(fmt.Sprintf("The Database %s is readonly", d.Name()))
}

func (d metadataDB) addTable(t sql.Table) {
	d.Database.AddTable(t)
}

type metadataTable struct {
	*memory.Table
}

func newTable(name string, schema sql.Schema, data memory.TableData) *metadataTable {
	return &metadataTable{
		memory.NewTable(name, schema, data),
	}
}

func (t *metadataTable) Insert(values ...interface{}) error {
	panic(fmt.Sprintf("The Database %s is readonly", t.Name()))
}

type metadataColumn struct {
	*sql.Column
	def interface{}
}

func schema(columns []metadataColumn) (schema sql.Schema, index map[string]int) {
	schema = make([]*sql.Column, len(columns))
	index = make(map[string]int)
	i := 0
	for _, f := range columns {
		schema[i] = f.Column
		index[f.Name] = i
		i++
	}

	return schema, index
}
