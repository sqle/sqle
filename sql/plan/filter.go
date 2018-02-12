package plan

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/sql"
)

type Filter struct {
	UnaryNode
	expression sql.Expression
}

func NewFilter(expression sql.Expression, child sql.Node) *Filter {
	return &Filter{
		UnaryNode:  UnaryNode{Child: child},
		expression: expression,
	}
}

func (p *Filter) String() string {
	return fmt.Sprintf("[Filter] %s::%s", p.Child.String(), p.expression.Name())
}

func (p *Filter) Resolved() bool {
	return p.UnaryNode.Child.Resolved() && p.expression.Resolved()
}

func (p *Filter) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &filterIter{p, i}, nil
}

func (p *Filter) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := p.UnaryNode.Child.TransformUp(f)
	n := NewFilter(p.expression, c)

	return f(n)
}

func (p *Filter) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := p.UnaryNode.Child.TransformExpressionsUp(f)
	e := p.expression.TransformUp(f)
	n := NewFilter(e, c)

	return n
}

type filterIter struct {
	f         *Filter
	childIter sql.RowIter
}

func (i *filterIter) Next() (sql.Row, error) {
	for {
		row, err := i.childIter.Next()

		if err != nil {
			return nil, err
		} else if i.f.expression.Eval(row) == true {
			return row, nil
		}
	}
}

func (i *filterIter) Close() error {
	return i.childIter.Close()
}
