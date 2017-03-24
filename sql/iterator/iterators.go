package iterator

import (
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
)

//-- DB
type DBIterator struct {
	databases []sql.Database
	idx       int
}

func NewDBIterator(databases sql.Catalog) *DBIterator {
	return &DBIterator{databases.Dbs(), 0}
}

func (i *DBIterator) Next() (sql.Database, error) {
	if i.idx >= len(i.databases) {
		return nil, io.EOF
	}

	i.idx++
	return i.databases[i.idx-1], nil
}

func (i *DBIterator) Close() error {
	i.databases = nil
	return nil
}

//-- Table
type TableIterator struct {
	tables []sql.Table
	idx    int
}

func NewTableIterator(db sql.Database) *TableIterator {
	return &TableIterator{
		tables: sql.TableSlice(db.Tables()),
	}
}

func (i *TableIterator) Next() (sql.Table, error) {
	if i.idx >= len(i.tables) {
		return nil, io.EOF
	}

	i.idx++
	return i.tables[i.idx-1], nil
}

func (i *TableIterator) Close() error {
	i.tables = nil
	return nil
}

//-- columns
type ColumnIterator struct {
	columns sql.Schema
	idx     int
}

func NewColumnIterator(table sql.Table) *ColumnIterator {
	return &ColumnIterator{
		columns: table.Schema(),
	}
}

func (i *ColumnIterator) Next() (*sql.Column, error) {
	if i.idx >= len(i.columns) {
		return &sql.Column{}, io.EOF
	}

	i.idx++
	return i.columns[i.idx-1], nil
}

func (i *ColumnIterator) Close() error {
	i.columns = nil
	return nil
}

//-- rows
type rowIterator struct {
	rows []sql.Row
	idx  int
}

func NewRowIterator(rows []sql.Row) sql.RowIter {
	return &rowIterator{rows: rows}
}

func (i *rowIterator) Next() (sql.Row, error) {
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}

	row := i.rows[i.idx]
	i.idx++
	return row.Copy(), nil
}

func (i *rowIterator) Close() error {
	i.rows = nil
	return nil
}
