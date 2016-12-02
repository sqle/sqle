package metadata

import (
	"sort"
	"testing"

	"gopkg.in/sqle/sqle.v0/memory"
	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/assert"
)

func TestTables(t *testing.T) {
	c := sql.NewCatalog()
	m := NewDB(c)
	c.AddDatabase(m)

	db1 := memory.NewDatabase("db1")
	db2 := memory.NewDatabase("db2")
	c.AddDatabase(db1)
	c.AddDatabase(db2)

	db1.AddTable(memory.NewTable("table11", sql.Schema{}, nil))
	db1.AddTable(memory.NewTable("table12", sql.Schema{}, nil))
	db2.AddTable(memory.NewTable("table21", sql.Schema{}, nil))
	db2.AddTable(memory.NewTable("table22", sql.Schema{}, nil))

	tablesTable, err := c.Table(SchemaDBname, SchemaTableTableName)
	assert.Nil(t, err)
	assert.NotNil(t, tablesTable)
	iter, err := tablesTable.RowIter()
	assert.Nil(t, err)
	assert.NotNil(t, iter)

	var names sort.StringSlice
	var expected = sort.StringSlice{
		SchemaColumnTableName, SchemaDBTableName, SchemaTableTableName,
		"table11", "table12", "table21", "table22",
	}
	for row, err := iter.Next(); err == nil; row, err = iter.Next() {
		names = append(names, row.Columns()[2].(string))
	}
	expected.Sort()
	names.Sort()
	assert.Equal(t, expected, names)
}
