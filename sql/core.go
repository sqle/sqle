package sql

import (
	"errors"
	"fmt"
)

type Nameable interface {
	Name() string
}

type Resolvable interface {
	Resolved() bool
}

type Transformable interface {
	TransformUp(func(Node) Node) Node
	TransformExpressionsUp(func(Expression) Expression) Node
}

type Expression interface {
	Resolvable
	Type() Type
	Name() string
	IsNullable() bool
	Eval(Row) interface{}
	TransformUp(func(Expression) Expression) Expression
}

// AggregationExpression implements an aggregation expression, where an
// aggregation buffer is created for each grouping (NewBuffer) and rows in the
// grouping are fed to the buffer (Update). Multiple buffers can be merged
// (Merge), making partial aggregations possible.
// Note that Eval must be called with the final aggregation buffer in order to
// get the final result.
type AggregationExpression interface {
	Expression
	// NewBuffer creates a new aggregation buffer and returns it as a Row.
	NewBuffer() Row
	// Update updates the given buffer with the given row.
	Update(buffer, row Row)
	// Merge merges a partial buffer into a global one.
	Merge(buffer, partial Row)
}

type Aggregation interface {
	Update(Row) (Row, error)
	Merge(Row)
	Eval() interface{}
}

type Node interface {
	fmt.Stringer
	Resolvable
	Transformable
	Schema() Schema
	Children() []Node
	RowIter() (RowIter, error)
}

type Table interface {
	Nameable
	Node
}

type Inserter interface {
	Insert(row Row) error
}

type Database interface {
	Nameable
	Table(string) (Table, error)
	Tables() map[string]Table
}

var ErrInvalidType = errors.New("invalid type")

func MapTableToSliceTable(tables map[string]Table) []Table {
	t := make([]Table, len(tables))
	i := 0
	for _, table := range tables {
		t[i] = table
		i++
	}

	return t
}
