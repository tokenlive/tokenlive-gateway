package repository

import (
	"os"
	"strings"
	"testing"
)

func TestSplitClickHouseDDLsFromSchemaFile(t *testing.T) {
	content, err := os.ReadFile("../../scripts/clickhouse_schema.sql")
	if err != nil {
		t.Fatalf("read clickhouse schema: %v", err)
	}

	ddls := splitClickHouseDDLs(string(content))
	if len(ddls) != 10 {
		t.Fatalf("expected 10 ddl statements, got %d", len(ddls))
	}

	for i, ddl := range ddls {
		if strings.Contains(ddl, ";") {
			t.Fatalf("ddl %d still contains semicolon: %q", i, ddl)
		}
		if strings.HasPrefix(strings.TrimSpace(ddl), "--") {
			t.Fatalf("ddl %d still contains line comment prefix: %q", i, ddl)
		}
	}
}
