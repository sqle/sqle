package metadata

import (
	"fmt"
	"io"

	"gopkg.in/sqle/sqle.v0/memory"
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
	index map[string]int
}

func newcolumnsTable(catalog sql.DBStorer) *columnsTable {
	schema, index := schema(metadataColumns)
	data := columnsData{data: catalog, index: index}
	return &columnsTable{
		newTable(SchemaColumnTableName, schema, data),
		index,
	}
}

func (t *columnsTable) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: %s is a table view; Insertion is not allowed", t.Name())
}

type columnsData struct {
	data  sql.DBStorer
	index map[string]int
}

func (c columnsData) IterData() memory.IteratorData {
	return &columnsDBIter{data: c.data.Dbs(), index: c.index}
}

func (c columnsData) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type columnsDBIter struct {
	data  []sql.Database
	index map[string]int
	cur   internalTableColumnIterator
	idx   int
	count *int
}

func (i *columnsDBIter) Length() int {
	if i.count == nil {
		count := 0
		for _, db := range i.data {
			tables := db.Tables()
			for _, t := range tables {
				count += len(t.Schema())
			}
		}
		i.count = &count
	}
	return *i.count
}

func (i *columnsDBIter) Get(idx int) []interface{} {
	next, _ := i.Next()
	return next
}

func (i *columnsDBIter) Next() ([]interface{}, error) {
	if i.cur == nil {
		i.cur = &cTblIterator{
			data: tables(i.data[i.idx].Tables()),
		}
	}

	if table, column, ord, err := i.cur.Next(); err == nil {
		return i.row(i.data[i.idx], table, column, ord), nil
	} else if i.idx < len(i.data) {
		i.cur = nil
		i.idx++
		return i.Next()
	}

	return nil, io.EOF
}

func (i *columnsDBIter) row(db sql.Database, table sql.Table, column *sql.Column, ord int) []interface{} {
	row := make([]interface{}, len(metadataColumns))
	k := 0
	for _, f := range metadataColumns {
		row[k] = i.getColumn(f.Name, db, table, column, ord)
		k++
	}

	return row
}

func (i *columnsDBIter) getColumn(name string, db sql.Database, table sql.Table, column *sql.Column, ord int) interface{} {
	switch name {
	case "table_schema":
		return db.Name()
	case "table_name":
		return table.Name()
	case "column_name":
		return column.Name
	case "ordinal_position":
		return int32(ord + 1)
	}

	return metadataColumns[i.index[name]].def
}

type internalTableColumnIterator interface {
	Next() (sql.Table, *sql.Column, int, error)
}

type cTblIterator struct {
	data []sql.Table
	cur  internalColumnIterator
	idx  int
}

func (i *cTblIterator) Next() (sql.Table, *sql.Column, int, error) {
	if i.cur == nil {
		i.cur = &cIterator{data: i.data[i.idx].Schema()}
	}

	if column, ord, err := i.cur.Next(); err == nil {
		return i.data[i.idx], column, ord, nil
	} else if i.idx < len(i.data)-1 {
		i.cur = nil
		i.idx++
		return i.Next()
	}

	return nil, &sql.Column{}, 0, io.EOF
}

type internalColumnIterator interface {
	Next() (*sql.Column, int, error)
}

type cIterator struct {
	data sql.Schema
	idx  int
}

func (i *cIterator) Next() (*sql.Column, int, error) {
	if i.idx >= len(i.data) {
		return &sql.Column{}, 0, io.EOF
	}

	i.idx++
	return i.data[i.idx-1], i.idx - 1, nil
}
