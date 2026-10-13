package stage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

var ErrWorkMismatch = errors.New("work file belongs to another build")

func CheckIdentity(ctx context.Context, db *sql.DB, want map[string]string) error {
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS build_identity (key VARCHAR PRIMARY KEY, value VARCHAR)"); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var bad []string
	for _, key := range keys {
		var have string
		err := db.QueryRowContext(ctx, "SELECT value FROM build_identity WHERE key = ?", key).Scan(&have)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := db.ExecContext(ctx, "INSERT INTO build_identity VALUES (?, ?)", key, want[key]); err != nil {
				return fmt.Errorf("identity: %w", err)
			}
		case err != nil:
			return fmt.Errorf("identity: %w", err)
		case have != want[key]:
			bad = append(bad, fmt.Sprintf("%s is %q, the work file has %q", key, want[key], have))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%w, delete work.duckdb to start over: %s", ErrWorkMismatch, strings.Join(bad, "; "))
	}
	return nil
}
