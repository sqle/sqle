package analyzer

import (
	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"
	"gopkg.in/sqle/sqle.v0/sql/plan"
)

var DefaultRules = []Rule{
	{"resolve_tables", resolveTables},
	{"resolve_columns", resolveColumns},
	{"resolve_database", resolveDatabase},
	{"resolve_star", resolveStar},
	{"resolve_functions", resolveFunctions},
}

func resolveDatabase(a *Analyzer, n sql.Node) sql.Node {
	_, ok := n.(*plan.ShowTables)
	if !ok {
		return n
	}

	db, err := a.Catalog.Database(a.CurrentDatabase)
	if err != nil {
		return n
	}

	return plan.NewShowTables(db)
}

func resolveTables(a *Analyzer, n sql.Node) sql.Node {
	return n.TransformUp(func(n sql.Node) sql.Node {
		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n
		}

		rt, err := a.Catalog.Table(a.CurrentDatabase, t.Name)
		if err != nil {
			return n
		}

		return rt
	})
}

func resolveStar(a *Analyzer, n sql.Node) sql.Node {
	return n.TransformUp(func(n sql.Node) sql.Node {
		if n.Resolved() {
			return n
		}

		p, ok := n.(*plan.Project)
		if !ok {
			return n
		}

		if len(p.Expressions) != 1 {
			return n
		}

		if _, ok := p.Expressions[0].(*expression.Star); !ok {
			return n
		}

		var exprs []sql.Expression
		for i, e := range p.Child.Schema() {
			gf := expression.NewGetField(i, e.Type, e.Name)
			exprs = append(exprs, gf)
		}

		return plan.NewProject(exprs, p.Child)
	})
}

func resolveColumns(a *Analyzer, n sql.Node) sql.Node {
	return n.TransformUp(func(n sql.Node) sql.Node {
		if n.Resolved() {
			return n
		}

		if len(n.Children()) != 1 {
			return n
		}

		child := n.Children()[0]
		if !child.Resolved() {
			return n
		}

		colMap := map[string]*expression.GetField{}
		for idx, child := range child.Schema() {
			if _, ok := colMap[child.Name]; ok {
				// There is no unambiguous resolution
				return n
			}

			colMap[child.Name] = expression.NewGetField(idx, child.Type, child.Name)
		}

		return n.TransformExpressionsUp(func(e sql.Expression) sql.Expression {
			uc, ok := e.(*expression.UnresolvedColumn)
			if !ok {
				return e
			}

			gf, ok := colMap[uc.Name()]
			if !ok {
				return e
			}

			return gf
		})
	})
}

func resolveFunctions(a *Analyzer, n sql.Node) sql.Node {
	return n.TransformUp(func(n sql.Node) sql.Node {
		if n.Resolved() {
			return n
		}

		return n.TransformExpressionsUp(func(e sql.Expression) sql.Expression {
			uf, ok := e.(*expression.UnresolvedFunction)
			if !ok {
				return e
			}

			n := uf.Name()
			f, err := a.Catalog.Function(n)
			if err != nil {
				return e
			}

			rf, err := f.Build(uf.Children...)
			if err != nil {
				return e
			}

			return rf
		})
	})
}
