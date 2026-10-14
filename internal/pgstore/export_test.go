package pgstore

import (
	"io/fs"
	"slices"
	"strings"

	"pm/internal/pgstore/sql"
)

// ReadMigration returns every migration file joined in order
func ReadMigration() (string, int, error) {
	files, err := fs.Glob(sql.FS, "*.sql")
	if err != nil {
		return "", 0, err
	}
	slices.Sort(files)
	var all strings.Builder
	for _, f := range files {
		b, err := sql.FS.ReadFile(f)
		if err != nil {
			return "", 0, err
		}
		all.Write(b)
		all.WriteByte('\n')
	}
	return all.String(), len(files), nil
}
