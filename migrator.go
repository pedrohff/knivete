package main

import (
	"context"
	"fmt"
	"time"
)

type Migrator interface {
	Apply(ctx context.Context, statement string) error
	InsertToAppliedMigrations(ctx context.Context, fileName string) error
	FileIsApplied(ctx context.Context, fileName string) (bool, error)
}

func NewMigrator(api KSQLAPI) Migrator {
	return &migrator{
		api: api,
	}
}

type migrator struct {
	api KSQLAPI
}

func (m migrator) Apply(ctx context.Context, statement string) error {
	return m.api.Exec(ctx, statement, StreamPropertiesOffsetEarliest)
}

func (m migrator) InsertToAppliedMigrations(ctx context.Context, fileName string) error {
	statement := fmt.Sprintf("insert into applied_migrations (filename, applied_at) values ('%s', '%s');", fileName, time.Now().Format(time.RFC3339Nano))
	return m.api.Exec(ctx, statement, StreamPropertiesOffsetEarliest)
}

func (m migrator) FileIsApplied(ctx context.Context, fileName string) (bool, error) {
	_, err := m.api.Query(ctx, fmt.Sprintf("select filename from applied_migrations_agg where filename = '%s';", fileName))
	// TODO check if error should be handled
	if err != nil {
		if err == QueryWithNoResults {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
