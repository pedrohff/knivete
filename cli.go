package main

import (
	"context"
	"fmt"
	"github.com/urfave/cli"
	"io/ioutil"
	"os"
	"time"
)

var app = cli.NewApp()

func init() {
	app.Name = "Knivete"
	app.Usage = "KSQL Swiss Knife"
	app.Author = "Pedro Feitosa"
	app.Version = "v0.1"
	app.Commands = commands()
}

func commands() []cli.Command {
	return []cli.Command{
		{
			Name:        "migrate",
			Aliases:     []string{"m"},
			Usage:       "knivete migrate --directory=scripts/ --server=http://localhost:8088",
			Description: "Applies SQL files to a KSQL Server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "directory",
					Usage:    "--directory=scripts/",
					Required: true,
				}, cli.StringFlag{
					Name:  "server",
					Usage: "--server=http://localhost:8088",
					// Value:    "http://192.168.27.136:8088",
					Required: true,
				}, cli.BoolFlag{
					Name:  "dry-run",
					Usage: "--dry-run",
				},
			},
			Action: migrate,
		},
	}
}

func migrate(c *cli.Context) error {
	ctx := context.Background()

	if c.Bool("dry-run") {
		ctx = context.WithValue(ctx, "dry-run", true)
	}

	init := time.Now()
	host := c.String("server")
	ksqlapi, err := NewKSQLAPI(host)
	if err != nil {
		fmt.Println(err)
		return cli.NewExitError(err, 1)
	}
	creator := NewMigrationStructureCreator(ksqlapi)
	migrationTableExists, err := creator.MigrationTableExists(ctx)
	if err != nil {
		fmt.Println(err)
		return cli.NewExitError(err, 1)
	}
	if !migrationTableExists {
		err := creator.Create(ctx)
		if err != nil {
			fmt.Println(err)
			return cli.NewExitError(err, 1)
		}
		return cli.NewExitError(err, 1)
	}

	migrator := NewMigrator(ksqlapi)
	dirName := c.String("directory")
	var dirInfo os.FileInfo
	if dirInfo, err = os.Stat(dirName); os.IsNotExist(err) {
		fmt.Println("does not exist")
		return cli.NewExitError(err, 1)
	}

	if !dirInfo.IsDir() {
		fmt.Printf("%s is not a directory\n", dirName)
		return cli.NewExitError(err, 1)
	}
	if string(dirName[len(dirName)-1]) != "/" {
		dirName += "/"
	}

	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		fmt.Println(err)
		return cli.NewExitError(err, 1)
	}
	migrationsApplied := 0
	for _, file := range files {
		fmt.Printf("applying file %s\n", file.Name())
		fmt.Printf("\treading content\n")
		readFile, err := ioutil.ReadFile(dirName + file.Name())
		if err != nil {
			fmt.Println(err)
			return cli.NewExitError(err, 1)
		}

		fmt.Printf("\tchecking if already applied\n")
		applied, err := migrator.FileIsApplied(ctx, file.Name())
		if err != nil {
			fmt.Println(err)
			continue
		}
		if applied {
			fmt.Printf("file already applied\n\n")
			continue
		}
		fmt.Printf("\tapplying on ksql server\n")

		err = migrator.Apply(ctx, string(readFile))
		if err != nil {
			fmt.Printf("error applying file %s : %v\n", file.Name(), err)
			continue
		}

		fmt.Printf("\tmarking migration as applied\n")
		err = migrator.InsertToAppliedMigrations(ctx, file.Name())
		if err != nil {
			fmt.Println(err)
			return cli.NewExitError(err, 1)
		}
		fmt.Println()
		migrationsApplied++
	}
	fmt.Printf("applied %d migrations in %d ms\n", migrationsApplied, time.Since(init).Milliseconds())
	return nil
}
