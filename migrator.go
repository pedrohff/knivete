package main

import (
	"context"
)

type Migrator interface {
	Apply(ctx context.Context, fileName string, statement string) error
	InsertToAppliedMigrations(ctx context.Context, fileName string) error
}
