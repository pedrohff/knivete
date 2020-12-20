package main

import (
	"context"
)

const (
	createMainTableSql = `create table applied_migrations (filename varchar primary key, applied_at varchar) with (kafka_topic='applied-migrations', value_format='json', partitions=1, replicas=1);`
	createAggregationTableSql = `create table applied_migrations_agg as select filename, count(filename) from applied_migrations group by filename;`
)

type MigrationStructureCreator interface {
	MigrationTableExists(ctx context.Context) (bool, error)
	Create(ctx context.Context) (error)
}
