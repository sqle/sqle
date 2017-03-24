package metadata

import (
	"fmt"
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
)

var metadataColumns = []metadataColumn{
	{&sql.Column{Name: "table_catalog", Type: sql.String, Default: "def", Nullable: false}, "def"},
	{&sql.Column{Name: "table_schema", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "table_name", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "column_name", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "ordinal_position", Type: sql.Integer, Default: nil, Nullable: false}, int32(2)},
	{&sql.Column{Name: "column_default", Type: sql.String, Default: nil, Nullable: true}, "nil"},
	{&sql.Column{Name: "is_nullable", Type: sql.String, Default: false, Nullable: false}, "nil"},
	{&sql.Column{Name: "data_type", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	//{&sql.Column{Name: "character_maximum_length", Type: sql.BigInteger}, int64(0)},
	//{&sql.Column{Name: "character_octet_length", Type: sql.BigInteger}, int64(0)},
	//{&sql.Column{Name: "numeric_precision", Type: sql.BigInteger}, int64(0)},
	//{&sql.Column{Name: "numeric_scale", Type: sql.BigInteger}, int64(0)},
	//{&sql.Column{Name: "datetime_precision", Type: sql.BigInteger}, int64(0)},
	{&sql.Column{Name: "character_set_name", Type: sql.String, Default: "utf8", Nullable: false}, "nil"},
	{&sql.Column{Name: "collation_name", Type: sql.String, Default: "utf8_general_ci", Nullable: false}, "nil"},
	//{&sql.Column{Name: "column_type", Type: sql.String}, "nil"},
	{&sql.Column{Name: "column_key", Type: sql.String, Default: nil, Nullable: true}, "nil"},
	{&sql.Column{Name: "extra", Type: sql.String, Default: nil, Nullable: true}, "nil"},
	//{&sql.Column{Name: "privileges", Type: sql.String}, "nil"},
	{&sql.Column{Name: "column_comment", Type: sql.String, Default: nil, Nullable: true}, "nil"},
	//{&sql.Column{Name: "generation_expression", Type: sql.String}, "nil"},
}

type columnsTable struct {
	*metadataTable
}

func newcolumnsTable(catalog sql.Catalog) *columnsTable {
	schema, index := schema(metadataColumns)
	underlaying := columnsData{data: catalog, index: index}
	return &columnsTable{
		newTable(SchemaColumnTableName, schema, underlaying),
	}
}

type columnsData struct {
	data  sql.Catalog
	index map[string]int
}

func (c columnsData) RowIter() sql.RowIter {
	return &columnsIter{columnIterator: newColumnIterator(c.data), index: c.index}
}

func (c columnsData) Insert(row sql.Row) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type columnsIter struct {
	columnIterator *columnIterator
	index          map[string]int
	closed         bool
}

func (i *columnsIter) Close() error {
	i.closed = true
	return nil
}

func (i *columnsIter) Next() (sql.Row, error) {
	if i.closed {
		return nil, io.EOF
	}

	if db, table, col, ord, err := i.columnIterator.next(); err == nil {
		return i.toRow(db, table, col, ord), nil
	}

	i.closed = true
	return nil, io.EOF
}

func (i *columnsIter) toRow(db sql.Database, table sql.Table, column *sql.Column, ord int) sql.Row {
	items := make([]interface{}, len(metadataColumns))
	for j, f := range metadataColumns {
		items[j] = i.getField(f.Name, db, table, column, ord)
	}

	return sql.NewRow(items...)
}

func (i *columnsIter) getField(fieldName string, db sql.Database, table sql.Table, column *sql.Column, ord int) interface{} {
	switch fieldName {
	case "table_schema":
		return db.Name()
	case "table_name":
		return table.Name()
	case "column_name":
		return column.Name
	case "ordinal_position":
		return int32(ord + 1)
	}

	return metadataColumns[i.index[fieldName]].def
}
