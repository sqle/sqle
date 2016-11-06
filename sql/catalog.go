package sql

import "fmt"

// Catalog holds databases, tables and functions.
type catalog struct {
	Databases
	FunctionRegistry
}

type DBStorer interface {
	Registrator
	Database(name string) (Database, error)
	AddDatabase(db Database) error
	Table(dbName string, tableName string) (Table, error)
}

// NewCatalog returns a new empty Catalog.
func NewCatalog() DBStorer {
	return &catalog{
		Databases:        Databases{},
		FunctionRegistry: NewFunctionRegistry()}
}

// Databases is a collection of Database.
type Databases []Database

// Database returns the Database with the given name if it exists.
func (d Databases) Database(name string) (Database, error) {
	for _, db := range d {
		if db.Name() == name {
			return db, nil
		}
	}

	return nil, fmt.Errorf("database not found: %s", name)
}

// Table returns the Table with the given name if it exists.
func (d Databases) Table(dbName string, tableName string) (Table, error) {
	db, err := d.Database(dbName)
	if err != nil {
		return nil, err
	}

	tables := db.Tables()
	table, found := tables[tableName]
	if !found {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	return table, nil
}

// AddDatabase adds a the passed database to the catalog and returns an error
// if it could not be done because its name is incorrect or it already exists
func (c *Databases) AddDatabase(db Database) error {
	if db.Name() == "" || db.Name() == "INFORMATION_SCHEMA" {
		return fmt.Errorf("database name is not correct")
	}

	if _, err := c.Database(db.Name()); err == nil {
		return fmt.Errorf("database %s already existent in Catalog", db.Name())
	}

	*c = append(*c, db)
	return nil
}
