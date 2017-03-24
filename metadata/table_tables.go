package metadata

import (
	"fmt"
	"io"
	"time"

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
}

func newTablesTable(catalog sql.Catalog) *tablesTable {
	schema, index := schema(metadataTables)
	underlaying := underlayingTableData{data: catalog, index: index}
	return &tablesTable{
		newTable(SchemaTableTableName, schema, underlaying),
	}
}

type underlayingTableData struct {
	data  sql.Catalog
	index map[string]int
}

func (c underlayingTableData) RowIter() sql.RowIter {
	return &tablesIter{
		tableIterator: newTableIterator(c.data),
		index:         c.index,
	}
}

func (c underlayingTableData) Insert(row sql.Row) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type tablesIter struct {
	tableIterator *tableIterator
	index         map[string]int
	closed        bool
}

func (i *tablesIter) Close() error {
	i.closed = true
	return nil
}

func (i *tablesIter) Next() (sql.Row, error) {
	if i.closed {
		return nil, io.EOF
	}

	if db, table, err := i.tableIterator.next(); err == nil {
		return i.toRow(db, table), nil
	}

	i.closed = true
	return nil, io.EOF
}

func (i *tablesIter) toRow(db sql.Database, table sql.Table) sql.Row {
	items := make([]interface{}, len(metadataTables))
	for j, f := range metadataTables {
		items[j] = i.getField(f.Name, db, table)
	}

	return sql.NewRow(items...)
}

func (i *tablesIter) getField(fieldName string, db sql.Database, value sql.Table) interface{} {
	switch fieldName {
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

	return metadataTables[i.index[fieldName]].def
}
