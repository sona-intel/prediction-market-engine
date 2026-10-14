package pgstore

import "pm/internal/pgstore/sql"

func ReadMigration() (string, error) {
	b, err := sql.FS.ReadFile("001_venues.sql")
	return string(b), err
}
