package metadata

import (
	"sort"
	"testing"

	"gopkg.in/sqle/sqle.v0/memory"
	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/assert"
)

func TestSchemata(t *testing.T) {
	c := sql.NewCatalog()
	m := NewDB(c)
	c.AddDatabase(m)

	db1 := memory.NewDatabase("db1")
	db2 := memory.NewDatabase("db2")
	c.AddDatabase(db1)
	c.AddDatabase(db2)

	dbTable, err := c.Table(SchemaDBname, SchemaDBTableName)
	assert.Nil(t, err)
	assert.NotNil(t, dbTable)
	iter, err := dbTable.RowIter()
	assert.Nil(t, err)
	assert.NotNil(t, iter)

	var names sort.StringSlice
	var expected = sort.StringSlice{SchemaDBname, "db1", "db2"}
	for row, err := iter.Next(); err == nil; row, err = iter.Next() {
		names = append(names, row.Columns()[1].(string))
	}
	expected.Sort()
	names.Sort()
	assert.Equal(t, expected, names)
}
