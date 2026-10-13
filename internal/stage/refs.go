package stage

import (
	"context"
	"fmt"

	"pm/internal/refs"
)

const (
	refsStage = "refs"
	refsItem  = "all"
)

func Refs(ctx context.Context, o Options) error {
	done, err := refsDone(ctx, o)
	if err != nil || done {
		return err
	}
	if err := refs.Build(ctx, o.Src, o.Work); err != nil {
		return err
	}
	_, err = o.Work.ExecContext(ctx, "INSERT OR REPLACE INTO build_state VALUES (?, ?, 'done', 0, 0, now())", refsStage, refsItem)
	return err
}

func refsDone(ctx context.Context, o Options) (bool, error) {
	conn, err := o.Work.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	if err := (&runner{conn: conn}).setup(ctx); err != nil {
		return false, err
	}
	done, err := doneItems(ctx, conn, refsStage)
	if err != nil {
		return false, fmt.Errorf("refs: %w", err)
	}
	return done[refsItem], nil
}
