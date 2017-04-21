package plan

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"

	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	require := require.New(t)

	data := []sql.Row{
		sql.NewRow("c", nil),
		sql.NewRow("a", int32(3)),
		sql.NewRow("b", int32(3)),
		sql.NewRow("c", int32(1)),
		sql.NewRow(nil, int32(1)),
	}

	schema := sql.Schema{
		{Name: "col1", Type: sql.String, Nullable: true},
		{Name: "col2", Type: sql.Integer, Nullable: true},
	}

	child := mem.NewTable("test", schema)
	for _, row := range data {
		require.NoError(child.Insert(row))
	}

	sf := []SortField{
		{Column: expression.NewGetField(1, sql.Integer, "col2", true), Order: Ascending, NullOrdering: NullsFirst},
		{Column: expression.NewGetField(0, sql.String, "col1", true), Order: Descending, NullOrdering: NullsLast},
	}
	s := NewSort(sf, child)
	require.Equal(schema, s.Schema())

	expected := []sql.Row{
		sql.NewRow("c", nil),
		sql.NewRow("c", int32(1)),
		sql.NewRow(nil, int32(1)),
		sql.NewRow("b", int32(3)),
		sql.NewRow("a", int32(3)),
	}

	actual, err := sql.NodeToRows(s)
	require.NoError(err)
	require.Equal(expected, actual)
}

func TestSortAscending(t *testing.T) {
	require := require.New(t)

	data := []sql.Row{
		sql.NewRow("c"),
		sql.NewRow("a"),
		sql.NewRow("d"),
		sql.NewRow(nil),
		sql.NewRow("b"),
	}

	schema := sql.Schema{
		{Name: "col1", Type: sql.String, Nullable: true},
	}

	child := mem.NewTable("test", schema)
	for _, row := range data {
		require.NoError(child.Insert(row))
	}

	sf := []SortField{
		{Column: expression.NewGetField(0, sql.String, "col1", true), Order: Ascending, NullOrdering: NullsFirst},
	}
	s := NewSort(sf, child)
	require.Equal(schema, s.Schema())

	expected := []sql.Row{
		sql.NewRow(nil),
		sql.NewRow("a"),
		sql.NewRow("b"),
		sql.NewRow("c"),
		sql.NewRow("d"),
	}

	actual, err := sql.NodeToRows(s)
	require.NoError(err)
	require.Equal(expected, actual)
}

func TestSortDescending(t *testing.T) {
	require := require.New(t)

	data := []sql.Row{
		sql.NewRow("c"),
		sql.NewRow("a"),
		sql.NewRow("d"),
		sql.NewRow(nil),
		sql.NewRow("b"),
	}

	schema := sql.Schema{
		{Name: "col1", Type: sql.String, Nullable: true},
	}

	child := mem.NewTable("test", schema)
	for _, row := range data {
		require.NoError(child.Insert(row))
	}

	sf := []SortField{
		{Column: expression.NewGetField(0, sql.String, "col1", true), Order: Descending, NullOrdering: NullsFirst},
	}
	s := NewSort(sf, child)
	require.Equal(schema, s.Schema())

	expected := []sql.Row{
		sql.NewRow(nil),
		sql.NewRow("d"),
		sql.NewRow("c"),
		sql.NewRow("b"),
		sql.NewRow("a"),
	}

	actual, err := sql.NodeToRows(s)
	require.NoError(err)
	require.Equal(expected, actual)
}
