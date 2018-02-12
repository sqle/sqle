package mem

import (
	"fmt"
	"strings"

	"gopkg.in/sqle/sqle.v0/sql"
)

type Database struct {
	name             string
	tables           map[string]sql.Table
	caseInsitiveness bool
}

func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: map[string]sql.Table{},
	}
}

func NewCIDatabase(name string) *Database {
	db := NewDatabase(name)
	db.caseInsitiveness = true
	return db
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) Engine() string {
	return "MEMORY"
}

func (d *Database) Table(tableName string) (sql.Table, error) {
	if d.caseInsitiveness {
		tableName = strings.ToLower(tableName)
	}

	if table, ok := d.tables[tableName]; ok {
		return table, nil
	}

	return nil, fmt.Errorf("table not found: %s", tableName)
}

func (d *Database) Tables() map[string]sql.Table {
	return d.tables
}

func (d *Database) AddTable(t sql.Table) error {
	var tableName string
	if d.caseInsitiveness {
		tableName = strings.ToLower(t.Name())
	} else {
		tableName = t.Name()
	}

	if _, ok := d.tables[tableName]; ok {
		return fmt.Errorf("table already exists: %s", t.Name())
	}

	d.tables[tableName] = t
	return nil
}
