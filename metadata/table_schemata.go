package metadata

import (
	"fmt"
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/iterator"
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
}

func newSchemataTable(catalog sql.Catalog) *schemataTable {
	schema, index := schema(metadataSchemata)
	underlaying := underlayingSchemataData{data: catalog, index: index}
	return &schemataTable{
		newTable(SchemaDBTableName, schema, underlaying),
	}
}

type underlayingSchemataData struct {
	data  sql.Catalog
	index map[string]int
}

func (c underlayingSchemataData) RowIter() sql.RowIter {
	return &schemataIter{
		dbIterator: iterator.NewDBIterator(c.data),
		index:      c.index,
	}
}

func (c underlayingSchemataData) Insert(row sql.Row) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type schemataIter struct {
	dbIterator *iterator.DBIterator
	index      map[string]int
	closed     bool
}

func (i *schemataIter) Close() error {
	i.closed = true
	return nil
}

func (i *schemataIter) Next() (sql.Row, error) {
	if i.closed {
		return nil, io.EOF
	}

	if db, err := i.dbIterator.Next(); err == nil {
		return i.toRow(db), nil
	}

	i.closed = true
	return nil, io.EOF
}

func (i *schemataIter) toRow(db sql.Database) sql.Row {
	items := make([]interface{}, len(metadataSchemata))
	for j, f := range metadataSchemata {
		items[j] = i.getField(f.Name, db)
	}

	return sql.NewRow(items...)
}

func (i *schemataIter) getField(fieldName string, database sql.Database) interface{} {
	switch fieldName {
	case "schema_name":
		return database.Name()
	default:
		return metadataSchemata[i.index[fieldName]].def
	}
}
