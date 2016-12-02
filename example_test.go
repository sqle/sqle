package sqle_test

import (
	dbsql "database/sql"
	"fmt"

	"gopkg.in/sqle/sqle.v0"
	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
)

func Example() {
	// Create a test memory database and register it to the default engine.
	sqle.DefaultEngine.AddDatabase(createTestDatabase())

	// Open a sql connection with the default engine.
	conn, err := dbsql.Open(sqle.DriverName, "")
	checkIfError(err)

	// Prepare a query.
	stmt, err := conn.Prepare(`SELECT name, count(*) FROM mytable
	WHERE name = 'John Doe'
	GROUP BY name`)
	checkIfError(err)

	// Get result rows.
	rows, err := stmt.Query()
	checkIfError(err)

	// Iterate results and print them.
	for {
		if !rows.Next() {
			break
		}

		name := ""
		count := int64(0)
		err := rows.Scan(&name, &count)
		checkIfError(err)

		fmt.Println(name, count)
	}
	checkIfError(rows.Err())

	// Output: John Doe 2
}

func checkIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func createTestDatabase() *mem.Database {
	db := mem.NewDatabase("test")
	table := mem.NewTable("mytable", sql.Schema{
		{Name: "name", Type: sql.String},
		{Name: "email", Type: sql.String},
	})
	db.AddTable(table)
	table.Insert(sql.NewRow("John Doe", "john@doe.com"))
	table.Insert(sql.NewRow("John Doe", "johnalt@doe.com"))
	table.Insert(sql.NewRow("Jane Doe", "jane@doe.com"))
	table.Insert(sql.NewRow("Evil Bob", "evilbob@gmail.com"))
	return db
}
