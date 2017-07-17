package metadata

import (
	"fmt"
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
)

var metadataSchemata = []metadataColumn{
	{&sql.Column{Name: "catalog_name", Type: sql.String, Default: nil, Nullable: false}, DefaultCatalog}, //TODO: ensure that catalog_name is always 'def'
	{&sql.Column{Name: "schema_name", Type: sql.String, Default: nil, Nullable: false}, getDbName},
	{&sql.Column{Name: "default_character_set_name", Type: sql.String, Default: "utf8", Nullable: false}, DefaultCharsetUtf8},          //TODO: ensure that characterSet is always 'utf-8'
	{&sql.Column{Name: "default_collation_name", Type: sql.String, Default: "utf8_general_ci", Nullable: false}, DefaultCollationUtf8}, //TODO: ensure that collation is always 'utf-8'
	{&sql.Column{Name: "sql_path", Type: sql.String, Default: nil, Nullable: true}, DefaultNul},                                        //TODO: ensure that sql_path is always 'null'
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
		dbIterator: sql.NewDBIterator(c.data),
		index:      c.index,
	}
}

func (c underlayingSchemataData) Insert(row sql.Row) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type schemataIter struct {
	dbIterator *sql.DBIterator
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
	fieldValue := metadataSchemata[i.index[fieldName]].val
	if fieldValueFunc, ok := fieldValue.(fieldByDb); ok {
		return fieldValueFunc(database)
	}

	return fieldValue
}
