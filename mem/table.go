package mem

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/sql"
)

type Table struct {
	name            string
	schema          sql.Schema
	underlayingData UnderlayingTableData
}

type UnderlayingTableData interface {
	RowIter() sql.RowIter
	Insert(row sql.Row) error
}

func NewTable(name string, schema sql.Schema) *Table {
	return &Table{
		name:            name,
		schema:          schema,
		underlayingData: &genericUnderlaying{},
	}
}

func NewTableWithUnderlaying(name string, schema sql.Schema, underlayingData UnderlayingTableData) *Table {
	return &Table{
		name:            name,
		schema:          schema,
		underlayingData: underlayingData,
	}
}

func (t *Table) String() string {
	return "[Table] " + t.Name()
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
	return t.underlayingData.RowIter(), nil
}

func (t *Table) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(t)
}

func (t *Table) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return t
}

func (t *Table) Insert(row sql.Row) error {
	if len(row) != len(t.schema) {
		return fmt.Errorf("insert expected %d values, got %d", len(t.schema), len(row))
	}

	for idx, value := range row {
		c := t.schema[idx]
		if !c.Check(value) {
			return sql.ErrInvalidType
		}
	}

	t.underlayingData.Insert(row)
	return nil
}

type genericUnderlaying struct {
	rows []sql.Row
}

func (u *genericUnderlaying) RowIter() sql.RowIter {
	return sql.RowsToRowIter(u.rows...)
}

func (u *genericUnderlaying) Insert(row sql.Row) error {
	u.rows = append(u.rows, row.Copy())
	return nil
}
