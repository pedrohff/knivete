package main

import (
	"context"
)

func isDryRun(ctx context.Context) bool {
	v := ctx.Value("dry-run")
	if v != nil {
		return v.(bool)
	}
	return false
}
