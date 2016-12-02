package memory

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/sql"
)

type Table struct {
	name   string
	schema sql.Schema
	data   TableData
}

type TableData interface {
	IterData() IteratorData
	Insert(values ...interface{}) error
}

func NewTable(name string, schema sql.Schema, data TableData) *Table {
	return &Table{
		name:   name,
		schema: schema,
		data:   data,
	}
}

func (Table) Resolved() bool {
	return true
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Schema() sql.Schema {
	return t.schema
}

func (t *Table) Children() []sql.Node {
	return []sql.Node{}
}

func (t *Table) RowIter() (sql.RowIter, error) {
	return NewIter(t.data.IterData()), nil
}

func (t *Table) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(t)
}

func (t *Table) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return t
}

func (t *Table) Insert(values ...interface{}) error {
	if len(values) != len(t.schema) {
		return fmt.Errorf("insert expected %d values, got %d", len(t.schema), len(values))
	}

	for idx, value := range values {
		f := t.schema[idx]
		if !f.Type.Check(value) {
			return sql.ErrInvalidType
		}
	}

	t.data.Insert(values...)
	return nil
}
