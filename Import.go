package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func main() {
	// Flags
	dataSource := flag.String("input", "/dataset", "Data to Import [STRING]")
	delimiters := flag.String("delimiters", ";:|", "delimiters list [STRING]")
	concurrency := flag.Int("concurrency", 10, "Concurrency (amount of GoRoutines) [INT]")
	copySize := flag.Int("copySize", 5000, "How many rows get imported per execution [INT]")
	dbUser := flag.String("dbUser", "pwned", "define DB username")
	dbName := flag.String("dbName", "pwned", "define DB name")
	// dbTable := flag.String("dbTable", "", "define DB table")
	dbPassword := flag.String("dbPassword", "123", "define DB password")
	dbHost := flag.String("dbHost", "192.168.178.206", "define DB host")
	flag.Parse()

	compiledRegex := regexp.MustCompile("^(.?)[" + *delimiters + "](.)$")

	lineChannel := make(chan string, 1000)
	filePathChannel := make(chan string, 100)
	currentGoroutinesChannel := make(chan int, *concurrency)
	stopToolChannel := make(chan bool, 1)
	stopFileWalkChannel := make(chan bool, 1)

	connStr := "host=" + *dbHost + " user="+ *dbUser + " dbname=" + *dbName + " password=" + *dbPassword + " sslmode=disable"
	fmt.Println(connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection Succesfull")

	fmt.Println("Starting Import at", time.Now().Format("02-Jan-2006 15:04:05"))
	defer timeTrack(time.Now(), "Txt To Postgres")

	go fileWalk(dataSource, filePathChannel, stopFileWalkChannel)

	close(filePathChannel)

	go textToPostgres(&lineChannel, *copySize, *db, &stopToolChannel)

	for {
		currentGoroutinesChannel <- 1
		path, morePaths := <-filePathChannel
		if !morePaths {
			log.Println("No more files to process")
			break
		}
		fmt.Println("processing file: ", path)
		go readFile(path, compiledRegex, lineChannel, currentGoroutinesChannel)
	}

	for {
		if len(currentGoroutinesChannel) == 0 {
			close(lineChannel)
			break
		}
	}

	<- stopFileWalkChannel
	close(filePathChannel)

	fmt.Println("Before reading from stopToolChannel")
	<-stopToolChannel

	fmt.Println("Stopping tool")
}

func readFile(path string, delimiters *regexp.Regexp, lineChannel chan string, currentGoroutinesChannel chan int) {

	fileData, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("Cannot read file %s", path)
		return
	}
	fileAsString := string(fileData)
	fileData = nil
	lines := strings.Split(fileAsString, "\n")
	fileAsString = ""

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			lineChannel <- delimiters.ReplaceAllString(line, "${1}:$2")
		}
	}
	log.Printf("Done reading %s", path)
	<-currentGoroutinesChannel

}

func fileWalk(dataSource *string, filePathChannel chan string, stopFileWalkChannel chan bool)  {
	_ = filepath.Walk(*dataSource,
		func(path string, file os.FileInfo, err error) error {
			if err != nil {
				log.Fatalf("Error reading %s: %v", path, err)
				return nil
			}
			if file.IsDir() {
				return nil
			}

			if filepath.Ext(file.Name()) == ".txt" {
				log.Printf("reading %s, %vB", path, file.Size())
				fmt.Println(path)
				filePathChannel <- path
			}
			return nil
		})

	stopFileWalkChannel <- true
}

func textToPostgres(lineChannel *chan string, copySize int, db sql.DB, stopToolChannel *chan bool) {

	const query = `
CREATE TABLE IF NOT EXISTS pwned (
	username varchar(300),
	password varchar(300)
)`

	_, err := db.Exec(query)
	if err != nil {
		log.Fatal("Failed to create table if exists")
	}

	lineCount := 0

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("pwned", "username", "password"))
	if err != nil {
		log.Fatal(err)
	}

	for {
		line, more := <-*lineChannel

		lineCount++;
		splitLine := strings.SplitN(line, ":", 2)

		if len(splitLine) == 2 {

			if lineCount%copySize == 0 {
				log.Printf("Commmiting %v lines", lineCount)

				_, err = stmt.Exec(splitLine[0], splitLine[1])
				err = txn.Commit()

				if err != nil {
					fmt.Println("Error on split Line")
					log.Fatal(err)
				}
				lineCount = 0
			}
		}

		if !more {
			fmt.Println("NO MORE LINES")
			log.Printf("Commmiting %v lines", lineCount)
			_, err = stmt.Exec()
			if err != nil {
				log.Fatal(err)
			}

			err = stmt.Close()
			if err != nil {
				log.Fatal(err)
			}

			err = txn.Commit()
			if err != nil {
				log.Fatal(err)
			}

			break
		}
	}
	*stopToolChannel <- true
}

func insertMD5() {
	fmt.Println("Is MD5 32 digit")
}

func insertSHA1() {
	fmt.Println("Is SHA1 40 digit")
}

func insertSHA224() {
	fmt.Println("Is SHA224 56 digit")
}

func insertSHA256() {
	fmt.Println("is SHA256 64 digit")
}

func insertSHA384() {
	fmt.Println("Is SHA384 96 digit")
}

func insertSHA512() {
	fmt.Println("IS SHA512 128 digit")
}

func insertRIPEMD160() {
	fmt.Println("Is RIPEMD160 40 digit")
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Println("Finished Import at", time.Now().Format("02-Jan-2006 15:04:05"))
	fmt.Printf("%s took %s", name, elapsed)
}