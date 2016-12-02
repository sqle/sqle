package metadata

import (
	"fmt"
	"io"
	"time"

	"gopkg.in/sqle/sqle.v0/memory"
	"gopkg.in/sqle/sqle.v0/sql"
)

var metadataTables = []metadataColumn{
	{&sql.Column{Name: "table_catalog", Type: sql.String, Default: nil, Nullable: false}, "def"},
	{&sql.Column{Name: "table_schema", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "table_name", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "table_type", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "engine", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	{&sql.Column{Name: "version", Type: sql.Integer, Default: nil, Nullable: true}, int32(0)},
	//{&sql.Column{"row_format", sql.String}, "nil"},
	{&sql.Column{Name: "table_rows", Type: sql.String, Default: nil, Nullable: false}, "nil"},
	//{&sql.Column{"avg_row_length", sql.BigInteger}, int64(0)},
	//{&sql.Column{"data_length", sql.BigInteger}, int64(0)},
	//{&sql.Column{"max_data_length", sql.BigInteger}, int64(0)},
	//{&sql.Column{"index_length", sql.BigInteger}, int64(0)},
	//{&sql.Column{"data_free", sql.BigInteger}, int64(0)},
	//{&sql.Column{"auto_increment", sql.BigInteger}, int64(0)},
	{&sql.Column{Name: "create_time", Type: sql.TimestampWithTimezone, Default: nil, Nullable: false}, time.Now()},
	{&sql.Column{Name: "update_time", Type: sql.TimestampWithTimezone, Default: nil, Nullable: false}, time.Time{}},
	//{&sql.Column{"check_time", sql.TimestampWithTimezone}, time.Time{}},
	{&sql.Column{Name: "table_collation", Type: sql.String, Default: "utf8_general_ci", Nullable: false}, "nil"},
	//{&sql.Column{"checksum", sql.String}, "nil"},
	//{&sql.Column{"create_options", sql.String}, "nil"},
	{&sql.Column{Name: "table_comment", Type: sql.String, Default: nil, Nullable: true}, "nil"},
}

type tablesTable struct {
	*metadataTable
	index map[string]int
}

func newTablesTable(catalog sql.DBStorer) *tablesTable {
	schema, index := schema(metadataTables)
	data := tablesData{data: catalog, index: index}
	return &tablesTable{
		newTable(SchemaTableTableName, schema, data),
		index,
	}
}

func (t *tablesTable) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: %s is a table view; Insertion is not allowed", t.Name())
}

type tablesData struct {
	data  sql.DBStorer
	index map[string]int
}

func (c tablesData) IterData() memory.IteratorData {
	return &tablesIter{data: c.data.Dbs(), index: c.index}
}

func (c tablesData) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type tablesIter struct {
	data  []sql.Database
	index map[string]int
	cur   internalTableIterator
	idx   int
	count *int
}

func (i *tablesIter) Length() int {
	if i.count == nil {
		count := 0
		for _, db := range i.data {
			count += len(db.Tables())
		}
		i.count = &count
	}

	return *i.count
}

func (i *tablesIter) Get(idx int) []interface{} {
	next, _ := i.Next()
	return next
}

func (i *tablesIter) Next() ([]interface{}, error) {
	if i.cur == nil {
		i.cur = &tIterator{data: tables(i.data[i.idx].Tables())}
	}

	if next, err := i.cur.Next(); err == nil {
		return i.row(i.data[i.idx], next), nil
	} else if i.idx < len(i.data)-1 {
		i.cur = nil
		i.idx++
		return i.Next()
	}

	return nil, io.EOF
}

func (i *tablesIter) row(db sql.Database, table sql.Table) []interface{} {
	row := make([]interface{}, len(metadataTables))
	k := 0
	for _, f := range metadataTables {
		row[k] = i.getColumn(f.Name, db, table)
		k++
	}

	return row
}

func (i *tablesIter) getColumn(name string, db sql.Database, value sql.Table) interface{} {
	switch name {
	case "table_schema":
		return db.Name()
	case "table_name":
		return value.Name()
	case "table_type":
		if db.Name() == SchemaDBname {
			return "View"
		}
		return "Base table"
	case "table_rows":
		if db.Name() == SchemaDBname {
			return "View"
		}
	case "engine":
		if db.Name() == SchemaDBname {
			return "Memory"
		}
	}
	return metadataTables[i.index[name]].def
}

type internalTableIterator interface {
	Next() (sql.Table, error)
}

type tIterator struct {
	data []sql.Table
	idx  int
}

func (i *tIterator) Next() (sql.Table, error) {
	if i.idx >= len(i.data) {
		return nil, io.EOF
	}

	i.idx++
	return i.data[i.idx-1], nil
}

func tables(tables map[string]sql.Table) []sql.Table {
	var t []sql.Table
	for _, table := range tables {
		t = append(t, table)
	}

	return t
}
