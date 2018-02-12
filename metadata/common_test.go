package metadata

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"strings"

	"github.com/stretchr/testify/assert"
)

func TestMetadataTables(t *testing.T) {
	metadataDB := NewDB(sql.NewCatalog())
	assert.Equal(t, SchemaDBname, metadataDB.Name())

	tables := metadataDB.Tables()
	assert.Contains(t, tables, strings.ToLower(SchemaDBTableName))
	assert.Contains(t, tables, strings.ToLower(SchemaTableTableName))
	assert.Contains(t, tables, strings.ToLower(SchemaColumnTableName))
	assert.Equal(t, 3, len(tables))
}
