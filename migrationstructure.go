package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	createMainTableSql        = `create table applied_migrations (filename varchar primary key, applied_at varchar) with (kafka_topic='applied-migrations', value_format='json', partitions=1, replicas=1);`
	createAggregationTableSql = `create table applied_migrations_agg as select filename, count(filename) from applied_migrations group by filename;`
)

type MigrationStructureCreator interface {
	MigrationTableExists(ctx context.Context) (bool, error)
	Create(ctx context.Context) error
}

func NewMigrationStructureCreator(api KSQLAPI) MigrationStructureCreator {
	return &migrationStructureCreator{api: api}
}

type migrationStructureCreator struct {
	api KSQLAPI
}

func (m migrationStructureCreator) MigrationTableExists(ctx context.Context) (bool, error) {
	if isDryRun(ctx) {
		fmt.Printf("[dry-run] skipping migration structure check\n")
		return true, nil
	}
	_, err := m.api.Describe(ctx, "describe applied_migrations;", nil)
	if err != nil {
		if strings.Contains(strings.ToUpper(err.Error()), strings.ToUpper("Could not find STREAM/TABLE 'APPLIED_MIGRATIONS' in the Metastore")) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m migrationStructureCreator) Create(ctx context.Context) error {
	_, err := m.api.CreateStream(ctx, createMainTableSql, nil)
	if err != nil {
		return err
	}
	duration := 30 * time.Second
	time.Sleep(duration)
	_, err = m.api.CreateStream(ctx, createAggregationTableSql, nil)
	if err != nil {
		return err
	}
	time.Sleep(duration)
	return nil
}
