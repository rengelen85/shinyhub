package main

import (
	"context"
	"time"

	"github.com/rvben/shinyhub/internal/db"
)

// Parent exit and cleanup completion are separate signals: allowing lifecycle
// changes between them could make recovery kill a newly draining generation.
func runRetiringParentCleanup(ctx context.Context, exited <-chan struct{}, apps []*db.App,
	acquire func(string) (func(), bool), cleanup func(*db.App) error, releaseFence func()) {
	select {
	case <-ctx.Done():
		return
	case <-exited:
	}
	for _, app := range apps {
		for {
			if ctx.Err() != nil {
				return
			}
			release, ok := acquire(app.Slug)
			if ok {
				err := cleanup(app)
				release()
				if err == nil {
					break
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
	if ctx.Err() == nil {
		releaseFence()
	}
}
