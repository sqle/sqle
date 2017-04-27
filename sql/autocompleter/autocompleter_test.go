package autocompleter

import (
	"database/sql"
	"strings"
	"testing"

	"gopkg.in/sqle/sqle.v0"
	"gopkg.in/sqle/sqle.v0/mem"
	sqli "gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/suite"
)

type AutocompleterTestSuite struct {
	suite.Suite

	ac *Autocompleter
}

func (suite *AutocompleterTestSuite) SetupSuite() {
	s := sqli.Schema{
		sqli.Column{"col1", sqli.String},
		sqli.Column{"col2", sqli.String},
		sqli.Column{"col3", sqli.Integer},
	}

	memDb := mem.NewDatabase("test")
	memDb.AddTable("testtable", mem.NewTable("testtable", s))
	sqle.DefaultEngine.AddDatabase(memDb)
	db, err := sql.Open(sqle.DriverName, "")
	suite.NoError(err)

	suite.ac = NewAutocompleter(db)
}

type fixture struct {
	q     string
	t     []string
	isErr bool
}

var fixtures []*fixture = []*fixture{{
	q: "SELECT",
	t: []string{"testtable.col1", "testtable.col2", "testtable.col3", "*"},
}, {
	q: "S",
	t: []string{"SELECT"},
}, {
	q: "SE",
	t: []string{"SELECT"},

}, {
	q: "se",
	t: []string{"SELECT"},
}, {
	q: "SEL",
	t: []string{"SELECT"},
}, {
	q: "SELECT ",
	t: []string{"testtable.col1", "testtable.col2", "testtable.col3", "*"},
},{
	q: "SELECT testtable.col1",
	t: []string{","},
}, {
	q: "SELECT testtable.col1,",
	t: []string{"testtable.col1", "testtable.col2", "testtable.col3"},
}, {
	q: "SELECT a,b,c FR",
	t: []string{"FROM"},
}, {
	q: "SELECT a,b,c FROM",
	t: []string{"testtable"},
}, {
	q: "SELECT a,b,c FROM testtable W",
	t: []string{"WHERE"},
}, {
	q: "SELECT a,b,c FROM testtable WHERE",
	t: []string{"testtable.col1", "testtable.col2", "testtable.col3"},
}, {
	q: "SELECT a,b,c FROM testtable WHERE 1=1",
	t: []string{"GROUP BY", "ORDER BY", "LIMIT"},
}, {
	q: "",
	t: []string{"SELECT"},
}, {
	q:     "BLA",
	t:     nil,
	isErr: true,
}}

func (suite *AutocompleterTestSuite) TestAutocompleter() {
	for _, f := range fixtures {
		suite.ac.Clear()

		next, err := suite.ac.Parse(f.q)
		if f.isErr {
			suite.Error(err)
			suite.True(strings.
				HasPrefix(err.Error(),
					"no autocomplete results for tokens:"))
		} else {
			suite.NoError(err)
		}

		suite.Equal(f.t, next)
	}

}

func TestAutocompleterTestSuite(t *testing.T) {
	suite.Run(t, new(AutocompleterTestSuite))
}
