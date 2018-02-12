package metadata

import (
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
)

//-- tables
type tableIterator struct {
	dbIterator           *sql.DBIterator
	currentDB            sql.Database
	currentTableIterator *sql.TableIterator
}

func newTableIterator(databases sql.Catalog) *tableIterator {
	return &tableIterator{
		dbIterator: sql.NewDBIterator(databases),
	}
}

func (i *tableIterator) next() (sql.Database, sql.Table, error) {
	if i.currentTableIterator == nil && i.setNextTableIterator() != nil {
		return nil, nil, io.EOF
	}

	if table, err := i.currentTableIterator.Next(); err == nil {
		return i.currentDB, table, nil
	}

	i.currentTableIterator = nil
	return i.next()
}

func (i *tableIterator) setNextTableIterator() error {
	if db, err := i.dbIterator.Next(); err == nil {
		i.currentDB = db
		i.currentTableIterator = sql.NewTableIterator(db)
		return nil
	}

	return io.EOF
}

// --- columns
type columnIterator struct {
	tableIterator         *tableIterator
	currentTable          sql.Table
	currentColumnIterator *sql.ColumnIterator
	colIdx                int
}

func newColumnIterator(databases sql.Catalog) *columnIterator {
	return &columnIterator{
		tableIterator: newTableIterator(databases),
	}
}

func (i *columnIterator) next() (sql.Database, sql.Table, *sql.Column, int, error) {
	if i.currentColumnIterator == nil && i.setNextColumnIterator() != nil {
		return nil, nil, &sql.Column{}, -1, io.EOF
	}

	if column, err := i.currentColumnIterator.Next(); err == nil {
		i.colIdx++
		return i.tableIterator.currentDB, i.currentTable, column, i.colIdx - 1, nil
	}

	i.currentColumnIterator = nil
	return i.next()
}

func (i *columnIterator) setNextColumnIterator() error {
	if _, table, err := i.tableIterator.next(); err == nil {
		i.currentTable = table
		i.colIdx = 0
		i.currentColumnIterator = sql.NewColumnIterator(table)
		return nil
	}

	return io.EOF
}
