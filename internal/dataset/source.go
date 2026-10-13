package dataset

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const treeURL = "https://huggingface.co/api/datasets/moose-code/polymarket-onchain-v1/tree/" + Revision + "?recursive=true"

var FullRows = map[string]int64{
	"order_filled":        1172658611,
	"redemption":          153914418,
	"split":               9649786,
	"merge":               11802739,
	"neg_risk_conversion": 2508492,
	"fee_refunded":        620731449,
	"condition":           1117152,
	"position":            2234304,
	"market_data":         1854758,
	"neg_risk_event":      26935,
	"wallet":              7362437,
	"fpmm_transaction":    2023553,
}

type Source struct {
	Root  string
	DB    *sql.DB
	files map[string][]string
}

func NewSource(ctx context.Context, db *sql.DB, root string) (*Source, error) {
	files, err := List(ctx, root)
	if err != nil {
		return nil, err
	}
	return &Source{Root: root, DB: db, files: files}, nil
}

func (s *Source) Files(table string) []string {
	return s.files[table]
}

func (s *Source) From(files []string, cols ...string) string {
	sel := "*"
	if len(cols) > 0 {
		sel = strings.Join(cols, ", ")
	}
	return "(SELECT " + sel + " FROM read_parquet(" + fileList(files) + ", hive_partitioning=false))"
}

func (s *Source) Rows(ctx context.Context, file string) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx, "SELECT num_rows FROM parquet_file_metadata(?)", file).Scan(&n)
	return n, err
}

func Check(ctx context.Context, s *Source, want map[string]int64) error {
	var errs []error
	for _, table := range slices.Sorted(maps.Keys(want)) {
		files := s.Files(table)
		var got int64
		if len(files) > 0 {
			err := s.DB.QueryRowContext(ctx,
				"SELECT CAST(sum(num_rows) AS BIGINT) FROM parquet_file_metadata("+fileList(files)+")").Scan(&got)
			if err != nil {
				return fmt.Errorf("footers of %s: %w", table, err)
			}
		}
		if got != want[table] {
			errs = append(errs, fmt.Errorf("%s has %d rows in %d files, want %d", table, got, len(files), want[table]))
		}
	}
	return errors.Join(errs...)
}

func LastOrderFilled(ctx context.Context, s *Source) (block, logIndex uint32, err error) {
	files := s.Files("order_filled")
	if len(files) == 0 {
		return 0, 0, errors.New("no order_filled files")
	}
	newest := files[len(files)-1]
	err = s.DB.QueryRowContext(ctx, `
		SELECT CAST(split_part(id, '_', 2) AS UINTEGER) AS b, CAST(split_part(id, '_', 3) AS UINTEGER) AS l
		FROM read_parquet(?, file_row_number = true, hive_partitioning = false)
		WHERE file_row_number >= (SELECT num_rows - 2000 FROM parquet_file_metadata(?))
		ORDER BY b DESC, l DESC LIMIT 1`, newest, newest).Scan(&block, &logIndex)
	return block, logIndex, err
}

func CountGets(ctx context.Context, db *sql.DB) (int, error) {
	n, _, err := Traffic(ctx, db)
	return n, err
}

func Traffic(ctx context.Context, db *sql.DB) (gets int, bytes int64, err error) {
	err = db.QueryRowContext(ctx, `
		SELECT count(*), coalesce(sum(try_cast(regexp_extract(message, 'content-length=(\d+)', 1) AS BIGINT)), 0)
		FROM duckdb_logs WHERE type = 'HTTP' AND contains(message, '''type'': GET')`).Scan(&gets, &bytes)
	if err != nil {
		return 0, 0, err
	}
	_, err = db.ExecContext(ctx, "CALL truncate_duckdb_logs()")
	return gets, bytes, err
}

func List(ctx context.Context, root string) (map[string][]string, error) {
	if IsRemote(root) {
		return listRemote(ctx, treeURL, root)
	}
	return listLocal(root)
}

func listLocal(root string) (map[string][]string, error) {
	var rels []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rels = append(rels, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return group(root, rels), nil
}

func listRemote(ctx context.Context, url, root string) (map[string][]string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list dataset files: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list dataset files: %s", resp.Status)
	}
	var entries []struct {
		Type string `json:"type"`
		Path string `json:"path"`
	}
	if err := json.UnmarshalRead(resp.Body, &entries); err != nil {
		return nil, fmt.Errorf("list dataset files: %w", err)
	}
	var rels []string
	for _, e := range entries {
		if e.Type == "file" && strings.HasSuffix(e.Path, ".parquet") {
			rels = append(rels, e.Path)
		}
	}
	return group(root, rels), nil
}

func group(root string, rels []string) map[string][]string {
	slices.Sort(rels)
	root = strings.TrimSuffix(root, "/")
	files := map[string][]string{}
	for _, rel := range rels {
		table, _, nested := strings.Cut(rel, "/")
		if !nested {
			table = strings.TrimSuffix(rel, ".parquet")
		}
		files[table] = append(files[table], root+"/"+rel)
	}
	return files
}

func fileList(files []string) string {
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = quote(f)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
