package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
)

func TestShowTables(t *testing.T) {
	assert := assert.New(t)

	unresolvedShowTables := NewShowTables(&sql.UnresolvedDatabase{})

	assert.False(unresolvedShowTables.Resolved())
	assert.Nil(unresolvedShowTables.Children())

	db := mem.NewDatabase("test")
	assert.Nil(db.AddTable(mem.NewTable("test1", nil)))
	assert.Nil(db.AddTable(mem.NewTable("test2", nil)))
	assert.Nil(db.AddTable(mem.NewTable("test3", nil)))

	resolvedShowTables := NewShowTables(db)
	assert.True(resolvedShowTables.Resolved())
	assert.Nil(resolvedShowTables.Children())

	iter, err := resolvedShowTables.RowIter()
	assert.Nil(err)

	res, err := iter.Next()
	assert.Nil(err)
	assert.Equal("test1", res[0])

	res, err = iter.Next()
	assert.Nil(err)
	assert.Equal("test2", res[0])

	res, err = iter.Next()
	assert.Nil(err)
	assert.Equal("test3", res[0])

	_, err = iter.Next()
	assert.Equal(io.EOF, err)
}
