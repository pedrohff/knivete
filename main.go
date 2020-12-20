package main

import (
	"context"
	"fmt"
)

func main() {
	ksqlapi, err := NewKSQLAPI("http://192.168.27.136:8088")
	if err != nil {
		panic(err)
	}
	query, err := ksqlapi.Query(context.Background(), "select filename from  migrationtestagg where filename='first.sql';")
	if err != nil {
		fmt.Println(err)
		return 
	}
	fmt.Println(query.Row.Columns[0])
}
