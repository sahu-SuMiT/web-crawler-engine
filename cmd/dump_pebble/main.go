package main

import (
	"fmt"
	"log"

	"github.com/cockroachdb/pebble"
)

func main() {
	db, err := pebble.Open("./data/pebble", &pebble.Options{})
	if err != nil {
		log.Fatalf("Failed to open Pebble DB: %v (Make sure crawler process is stopped)", err)
	}
	defer db.Close()

	iter, err := db.NewIter(nil)
	if err != nil {
		log.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Close()

	count := 0
	fmt.Println("==========================================================")
	fmt.Println("       PEBBLE DB CONTENTS DUMP")
	fmt.Println("==========================================================")

	for iter.First(); iter.Valid(); iter.Next() {
		count++
		fmt.Printf("[%3d] KEY   : %s\n      VALUE : %s\n\n", count, string(iter.Key()), string(iter.Value()))
	}

	if err := iter.Error(); err != nil {
		log.Printf("Iterator error: %v", err)
	}

	fmt.Println("==========================================================")
	fmt.Printf("Total Records Found: %d\n", count)
	fmt.Println("==========================================================")
}
