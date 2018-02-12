package metadata

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/mem"
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

const (
	DefaultCharsetUtf8   = "utf8"
	DefaultCollationUtf8 = "utf8_general_ci"
	DefaultCatalog       = "def"
)

var DefaultNul = interface{}(nil)

type metadataDB struct {
	mem.Database
	catalog sql.Catalog
}

func NewDB(catalog sql.Catalog) sql.Database {
	embeddedDB := mem.NewCIDatabase(SchemaDBname)
	m := &metadataDB{
		Database: *embeddedDB,
		catalog:  catalog,
	}

	m.addTable(newSchemataTable(catalog))
	m.addTable(newTablesTable(catalog))
	m.addTable(newcolumnsTable(catalog))
	return m
}

func (d *metadataDB) AddTable(t sql.Table) error {
	panic(fmt.Sprintf("The Database %s is readonly", d.Name()))
}

func (d *metadataDB) addTable(t sql.Table) error {
	return d.Database.AddTable(t)
}

type metadataTable struct {
	*mem.Table
}

func newTable(name string, schema sql.Schema, underlayingData mem.UnderlayingTableData) *metadataTable {
	return &metadataTable{
		mem.NewTableWithUnderlaying(name, schema, underlayingData),
	}
}

func (t *metadataTable) Insert(values ...interface{}) error {
	panic(fmt.Sprintf("The Database %s is readonly", t.Name()))
}

type metadataColumn struct {
	*sql.Column
	val interface{}
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

type fieldByDb func(db sql.Database) interface{}

var getDbName fieldByDb = func(db sql.Database) interface{} {
	return db.Name()
}

type fieldByTable func(table sql.Table) interface{}

var getTableName fieldByTable = func(table sql.Table) interface{} {
	return table.Name()
}

type fieldByDbAndTable func(database sql.Database, table sql.Table) interface{}

type RowCounter interface {
	RowCount() int64
}

type Enginer interface {
	Engine() string
}
