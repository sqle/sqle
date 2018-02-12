package plan

import (
	"fmt"
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
)

type Describe struct {
	UnaryNode
}

func NewDescribe(child sql.Node) *Describe {
	return &Describe{UnaryNode{child}}
}

func (d *Describe) String() string {
	return fmt.Sprintf("[Describe] %s", d.Child.String())
}

func (d *Describe) Schema() sql.Schema {
	return sql.Schema{{
		Name: "name",
		Type: sql.String,
	}, {
		Name: "type",
		Type: sql.String,
	}}
}

func (d *Describe) RowIter() (sql.RowIter, error) {
	return &describeIter{schema: d.Child.Schema()}, nil
}

func (d *Describe) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := d.UnaryNode.Child.TransformUp(f)
	n := NewDescribe(c)

	return f(n)
}

func (d *Describe) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := d.UnaryNode.Child.TransformExpressionsUp(f)
	n := NewDescribe(c)

	return n
}

type describeIter struct {
	schema sql.Schema
	i      int
}

func (i *describeIter) Next() (sql.Row, error) {
	if i.i >= len(i.schema) {
		return nil, io.EOF
	}

	f := i.schema[i.i]
	i.i++
	return sql.NewRow(f.Name, f.Type.Name()), nil
}

func (i *describeIter) Close() error {
	return nil
}
