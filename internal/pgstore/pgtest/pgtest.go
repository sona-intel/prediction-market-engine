// Package pgtest gives tests a migrated Postgres schema of their own
package pgtest

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"pm/internal/pgstore"
)

// EnvDSN names a database where tests may create and drop their own schemas
const EnvDSN = "PM_TEST_PG"

// New skips unless PM_TEST_PG is set, and makes a migrated schema that is dropped after the test
func New(t testing.TB) *pgstore.Store {
	t.Helper()
	dsn := os.Getenv(EnvDSN)
	if dsn == "" {
		t.Skip(EnvDSN + " is not set")
	}
	schema := "t_" + strconv.FormatUint(rand.Uint64(), 16)
	st, err := pgstore.Connect(t.Context(), dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := st.Pool.Exec(ctx, "drop schema if exists "+pgx.Identifier{schema}.Sanitize()+" cascade"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		st.Close()
	})
	if _, err := st.Pool.Exec(t.Context(), "create schema "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return st
}
