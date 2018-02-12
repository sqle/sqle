package analyzer

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/plan"
)

var DefaultValidationRules = []ValidationRule{
	{"validate_resolved", validateIsResolved},
	{"validate_order_by", validateOrderBy},
}

func validateIsResolved(a *Analyzer, n sql.Node) error {
	if !n.Resolved() {
		return fmt.Errorf("plan is not resolved :: %s", n)
	}

	return nil
}

func validateOrderBy(a *Analyzer, n sql.Node) error {
	switch n := n.(type) {
	case *plan.Sort:
		for _, field := range n.SortFields {
			switch field.Column.(type) {
			case sql.AggregationExpression:
				return fmt.Errorf("OrderBy does not support aggregation expressions :: %s", n)
			}
		}
	}

	return nil
}
