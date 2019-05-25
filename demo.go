package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/lib/pq"
	_ "io/ioutil"
	"log"
	_ "os"
	_ "path/filepath"
	_ "regexp"
	_ "strings"
	_ "time"
)

func main() {
	dbUser := flag.String("dbUser", "pwned", "define DB username")
	dbName := flag.String("dbName", "pwned", "define DB name")
	// dbTable := flag.String("dbTable", "", "define DB table")
	dbPassword := flag.String("dbPassword", "123", "define DB password")
	dbHost := flag.String("dbHost", "192.178.178.206", "define DB host")
	flag.Parse()


	connStr := "host=" + *dbHost + " user="+ *dbUser + " dbname=" + *dbName + " password=" + *dbPassword + " sslmode=disable"
	fmt.Println(connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	const query = `
CREATE TABLE IF NOT EXISTS pwned (
	username varchar(300),
	password varchar(300)
)`
	txn, err := db.Begin()
	_, err = txn.Prepare(pq.CopyIn("pwned", "username", "password"))

	if err != nil {
		fmt.Println("Fucked up")
	}



	_, err = db.Exec(query)
}
