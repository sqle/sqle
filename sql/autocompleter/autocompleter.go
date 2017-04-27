package autocompleter

import (
	"bytes"
	"database/sql"
	"fmt"
	"go/token"
	"strings"

	"gopkg.in/sqle/vitess-go.v2/vt/sqlparser"
)

const (
	showTablesQuery    = "SHOW TABLES"
	describeTableQuery = "DESCRIBE TABLE %s"

	dot = token.Token(46)
	comma = token.Token(44)

	anyToken = token.Token(-1)
	anyTokens = token.Token(-2)
)

type tokens []*tData

func (t tokens) String() string {
	buff := bytes.NewBuffer(nil)
	for _, tok := range t {
		buff.WriteString(fmt.Sprintf("[t: %d | d: %s]", tok.t, string(tok.data)))
	}

	return buff.String()
}

func (t tokens) CouldBeLastToken(s string) bool {
	l := t.Last()
	if l == nil {
		return false
	}

	return l.t == sqlparser.ID && strings.HasPrefix(s, strings.ToUpper(string(l.data)))
}

func (t tokens) Match(pattern ...token.Token) bool {
	n := len(t)
	m := len(pattern)

	if m == 0 {
		return n == 0
	}

	lookup := make([][]bool, n+1)
	for i := range lookup {
		lookup[i] = make([]bool, m+1)
	}

	lookup[0][0] = true

	for j := 1; j <= m; j++ {
		if pattern[j-1] == anyTokens {
			lookup[0][j] = lookup[0][j-1]
		}
	}

	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			if pattern[j-1] == anyTokens {
				lookup[i][j] = lookup[i][j-1] || lookup[i-1][j]
			} else if pattern[j-1] == anyToken ||
				t[i-1].t == pattern[j-1] {
				lookup[i][j] = lookup[i-1][j-1]
			} else {
				lookup[i][j] = false
			}
		}
	}

	return lookup[n][m]
}

func (t tokens) LastToken() token.Token {
	l := t.Last()
	if l == nil {
		return token.ILLEGAL
	}

	return l.t
}

func (t tokens) Last() *tData {
	if len(t) == 0 {
		return nil
	}

	return t[len(t)-1]
}

type tData struct {
	t    token.Token
	data []byte
}

type ErrNotProcess struct {
	Tokens tokens
}

func (e *ErrNotProcess) Error() string {
	return fmt.Sprintf("no autocomplete results for tokens: %s", e.Tokens)
}

type Autocompleter struct {
	db *sql.DB

	cache map[string][]string
}

func NewAutocompleter(db *sql.DB) *Autocompleter {
	return &Autocompleter{
		db: db,
	}
}

func (a *Autocompleter) Clear() {
	a.cache = nil
}

func (a *Autocompleter) Parse(s string) (out []string, err error) {
	tokens := a.tokens(s)
	switch {
	case tokens.Match(sqlparser.SELECT, sqlparser.ID, dot, sqlparser.ID):
		out = []string{","}
		break
	case tokens.Match(sqlparser.SELECT, anyTokens, comma):
		out, err = a.columns()
		break
	case tokens.CouldBeLastToken("SELECT") || tokens.Match():
		out = []string{"SELECT"}
		break
	case tokens.CouldBeLastToken("FROM"):
		out = []string{"FROM"}
		break
	case tokens.CouldBeLastToken("WHERE"):
		out = []string{"WHERE"}
		break
	case tokens.Match(anyTokens, sqlparser.FROM):
		out, err = a.tables()
		break
	case tokens.Match(sqlparser.SELECT):
		out, err = a.columns()
		if err == nil {
			out = append(out, "*")
		}
		break
	case tokens.Match(anyTokens, sqlparser.WHERE):
		out, err = a.columns()
		break
	case tokens.Match(anyTokens, sqlparser.WHERE, anyTokens):
		out = []string{"GROUP BY", "ORDER BY", "LIMIT"}
		break
	default:
		err = &ErrNotProcess{tokens}
	}

	return
}

func (a *Autocompleter) tokens(s string) tokens {
	tokenizer := sqlparser.NewStringTokenizer(s)
	var tokens tokens
	for {
		n, d := tokenizer.Scan()
		if n == 0 {
			break
		}

		tokens = append(tokens, &tData{token.Token(n), d})
	}

	return tokens
}

func (a *Autocompleter) tables() ([]string, error) {
	if err := a.fill(); err != nil {
		return nil, err
	}

	var result []string
	for k := range a.cache {
		result = append(result, k)
	}

	return result, nil
}

func (a *Autocompleter) columns() ([]string, error) {
	if err := a.fill(); err != nil {
		return nil, err
	}

	var result []string
	for table, cols := range a.cache {
		for _, col := range cols {
			result = append(result, fmt.Sprintf("%s.%s", table, col))
		}
	}
	return result, nil
}

func (a *Autocompleter) queryToSlice(query string) ([]string, error) {
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colLen := len(cols)

	vals := make([]interface{}, colLen)
	valPtrs := make([]interface{}, colLen)
	for i := 0; i < colLen; i++ {
		valPtrs[i] = &vals[i]
	}

	var result []string
	for {
		if !rows.Next() {
			break
		}

		if err := rows.Scan(valPtrs...); err != nil {
			return nil, err
		}

		result = append(result, fmt.Sprintf("%v", vals[0]))
	}

	return result, rows.Err()
}

func (a *Autocompleter) fill() error {
	if a.cache != nil {
		return nil
	}

	tableNames, err := a.queryToSlice(showTablesQuery)
	if err != nil {
		return err
	}

	tableData := make(map[string][]string)
	for _, tn := range tableNames {
		cols, err := a.queryToSlice(fmt.Sprintf(describeTableQuery, tn))
		if err != nil {
			return err
		}

		tableData[tn] = cols
	}

	a.cache = tableData

	return nil
}
