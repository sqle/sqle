package memory

import (
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
)

type IteratorData interface {
	Get(idx int) []interface{}
	Length() int
}

type Iter struct {
	idx  int
	data IteratorData
}

func NewIter(data IteratorData) *Iter {
	iter := &Iter{data: data}
	return iter
}

func (i *Iter) Next() (sql.Row, error) {
	if i.idx >= i.data.Length() {
		return nil, io.EOF
	}

	row := sql.NewRow(i.data.Get(i.idx)...)
	i.idx++
	return row.Copy(), nil
}

func (i *Iter) Close() error {
	i.data = nil
	return nil
}
