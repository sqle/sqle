package sql

import "io"

//-- DB
type DBIterator struct {
	databases []Database
	idx       int
}

func NewDBIterator(databases Catalog) *DBIterator {
	return &DBIterator{databases.Dbs(), 0}
}

func (i *DBIterator) Next() (Database, error) {
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
	tables []Table
	idx    int
}

func NewTableIterator(db Database) *TableIterator {
	return &TableIterator{
		tables: MapTableToSliceTable(db.Tables()),
	}
}

func (i *TableIterator) Next() (Table, error) {
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
	columns Schema
	idx     int
}

func NewColumnIterator(table Table) *ColumnIterator {
	return &ColumnIterator{
		columns: table.Schema(),
	}
}

func (i *ColumnIterator) Next() (*Column, error) {
	if i.idx >= len(i.columns) {
		return &Column{}, io.EOF
	}

	i.idx++
	return i.columns[i.idx-1], nil
}

func (i *ColumnIterator) Close() error {
	i.columns = nil
	return nil
}
