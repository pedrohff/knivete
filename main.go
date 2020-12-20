package main

import (
	"context"
	"fmt"
)

func main() {
	ksqlapi, err := NewKSQLAPI("http://192.168.27.136:8088")
	if err != nil {
		fmt.Println(err)
		return
	}
	creator := NewMigrationStructureCreator(ksqlapi)
	exists, err := creator.MigrationTableExists(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	if !exists {
		err := creator.Create(context.Background())
		if err != nil {
			fmt.Println(err)
			return
		}
		return
	}
	fmt.Println("already exists!")
}
