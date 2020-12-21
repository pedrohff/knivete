package main

import (
	"fmt"
	"os"
)

func main() {
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}
