package mem

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/assert"
)

func TestTable_Name(t *testing.T) {
	assert := assert.New(t)
	s := sql.Schema{
		{"col1", sql.String, nil, true},
	}
	table := NewTable("test", s)
	assert.Equal("test", table.Name())
}

func TestTable_Insert_RowIter(t *testing.T) {
	assert := assert.New(t)
	s := sql.Schema{
		{"col1", sql.String, nil, true},
	}

	table := NewTable("test", s)

	rows, err := sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 0)

	err = table.Insert(sql.NewRow("foo"))
	rows, err = sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 1)
	assert.Nil(s.CheckRow(rows[0]))

	err = table.Insert(sql.NewRow("bar"))
	rows, err = sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 2)
	assert.Nil(s.CheckRow(rows[0]))
	assert.Nil(s.CheckRow(rows[1]))
}
