package sqle_test

import (
	gosql "database/sql"
	"testing"

	"gopkg.in/sqle/sqle.v0"
	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/require"
)

const (
	driverName = "engine_tests"
)

func TestQueries(t *testing.T) {
	e := newEngine(t)

	testQuery(t, e,
		"SELECT i FROM mytable;",
		[][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE i = 2;",
		[][]interface{}{{int64(2)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable ORDER BY i DESC;",
		[][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC LIMIT 1;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) FROM mytable;",
		[][]interface{}{{int64(3)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) FROM mytable LIMIT 1;",
		[][]interface{}{{int64(3)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) AS c FROM mytable;",
		[][]interface{}{{int64(3)}},
	)
}

func TestInsertInto(t *testing.T) {
	e := newEngine(t)
	testQuery(t, e,
		"INSERT INTO mytable (s, i) VALUES ('x', 999);",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'x';",
		[][]interface{}{{int64(999)}},
	)
}

func testQuery(t *testing.T, e *sqle.Engine, q string, r [][]interface{}) {
	t.Run(q, func(t *testing.T) {
		assert := require.New(t)

		sqle.DefaultEngine = e

		db, err := gosql.Open(sqle.DriverName, "")
		assert.NoError(err)
		defer func() { assert.NoError(db.Close()) }()

		res, err := db.Query(q)
		assert.NoError(err)
		defer func() { assert.NoError(res.Close()) }()

		cols, err := res.Columns()
		assert.NoError(err)
		assert.Equal(len(r[0]), len(cols))

		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			valPtrs[i] = &vals[i]
		}

		i := 0
		for {
			if !res.Next() {
				break
			}

			err := res.Scan(valPtrs...)
			assert.NoError(err)

			assert.Equal(r[i], vals)
			i++
		}

		assert.NoError(res.Err())
		assert.Equal(len(r), i)
	})
}

func newEngine(t *testing.T) *sqle.Engine {
	assert := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.BigInteger},
		{Name: "s", Type: sql.String},
	})
	assert.Nil(table.Insert(sql.NewRow(int64(1), "a")))
	assert.Nil(table.Insert(sql.NewRow(int64(2), "b")))
	assert.Nil(table.Insert(sql.NewRow(int64(3), "c")))

	db := mem.NewDatabase("mydb")
	assert.Nil(db.AddTable(table))

	e := sqle.New()
	assert.Nil(e.AddDatabase(db))

	return e
}

func TestTable(t *testing.T) {
	var r sql.Table
	var err error

	assert := require.New(t)

	db1 := mem.NewDatabase("db1")
	assert.Nil(db1.AddTable(mem.NewTable("table11", sql.Schema{})))
	assert.Nil(db1.AddTable(mem.NewTable("table12", sql.Schema{})))
	db2 := mem.NewDatabase("db2")
	assert.Nil(db2.AddTable(mem.NewTable("table21", sql.Schema{})))
	assert.Nil(db2.AddTable(mem.NewTable("table22", sql.Schema{})))

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db1)
	catalog.AddDatabase(db2)

	r, err = catalog.Table("db1", "table11")
	assert.Equal("table11", r.Name())

	r, err = catalog.Table("db2", "table22")
	assert.Equal("table22", r.Name())

	r, err = catalog.Table("db1", "table22")
	assert.Error(err)
}
