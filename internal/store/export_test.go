package store

import "context"

func (s *Store) Query(ctx context.Context, stmt string) (string, error) {
	var v string
	err := s.read(func(h *handle) error { return h.db.QueryRowContext(ctx, stmt).Scan(&v) })
	return v, err
}
