package metadata

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
)

func TestColumns(t *testing.T) {
	c := sql.NewCatalog()
	m := NewDB(c)
	c.AddDatabase(m)

	db1 := mem.NewDatabase("db1")
	db2 := mem.NewDatabase("db2")
	c.AddDatabase(db1)
	c.AddDatabase(db2)

	sq11 := sql.Schema{&sql.Column{"table11c1", nil, false, false}, &sql.Column{"table11c1", nil, false, false}}
	sq21 := sql.Schema{&sql.Column{"table21c1", nil, false, false}}
	sq22 := sql.Schema{&sql.Column{"table22c1", nil, false, false}}

	db1.AddTable(mem.NewTable("table11", sq11))
	db2.AddTable(mem.NewTable("table21", sq21))
	db2.AddTable(mem.NewTable("table22", sq22))

	columnsTable, err := c.Table(SchemaDBname, SchemaColumnTableName)
	assert.Nil(t, err)
	assert.NotNil(t, columnsTable)
	iter, err := columnsTable.RowIter()
	assert.Nil(t, err)
	assert.NotNil(t, iter)

	var names sort.StringSlice
	var expected = sort.StringSlice{
		"catalog_name", "character_set_name", "collation_name", "column_comment", "column_default", "column_key",
		"column_name", "create_time", "data_type", "default_character_set_name", "default_collation_name",
		"engine", "extra", "is_nullable", "ordinal_position", "schema_name", "sql_path", "table_catalog",
		"table_catalog", "table_collation", "table_comment", "table_name", "table_name", "table_rows",
		"table_schema", "table_schema", "table_type", "update_time", "version",
		"table11c1", "table11c1", "table21c1", "table22c1",
	}
	for row, err := iter.Next(); err == nil; row, err = iter.Next() {
		names = append(names, row.Columns()[3].(string))
	}
	expected.Sort()
	names.Sort()
	assert.Equal(t, expected, names)
}
