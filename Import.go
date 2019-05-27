package main

import (
	"database/sql"
	"flag"
	"github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

func main() {
	// Flags
	input := flag.String("input", "/Users/joshuahemmings/Documents/Dev/Personal/GoTxtToPostgres/testDocuments", "Data to Import [STRING]")
	delimiters := flag.String("delimiters", ";:|", "delimiters list [STRING]")
	concurrency := flag.Int("concurrency", 10, "Concurrency (amount of GoRoutines) [INT]")
	copySize := flag.Int("copySize", 100, "How many rows get imported per execution [INT]")
	dbUser := flag.String("dbUser", "pwned", "define DB username")
	dbName := flag.String("dbName", "pwned", "define DB name")
	// dbTable := flag.String("dbTable", "", "define DB table")
	dbPassword := flag.String("dbPassword", "123", "define DB password")
	dbHost := flag.String("dbHost", "192.168.178.206", "define DB host")
	flag.Parse()

	compiledRegex := regexp.MustCompile("^(.*?)["+ *delimiters +"](.*)$")

	lineChannel := make(chan string, 1000)
	filePathChannel := make(chan string, 100)
	currentGoroutinesChannel := make(chan int, *concurrency)
	stopToolChannel := make(chan bool, 1)
	stopFileWalkChannel := make(chan bool, 1)

	numberOfTxtFiles := 0
	numberOfProcessedFiles := 0

	connStr := "host=" + *dbHost + " user=" + *dbUser + " dbname=" + *dbName + " password=" + *dbPassword + " sslmode=disable"
	log.Println(connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connection Succesfull")

	log.Println("Starting Import at", time.Now().Format("02-Jan-2006 15:04:05"))
	defer timeTrack(time.Now(), "Txt To Postgres")

	_ = filepath.Walk(*input,
		func(path string, file os.FileInfo, err error) error {
			if err != nil {
				log.Fatalf("Error reading %s: %v", path, err)
				return nil
			}
			if file.IsDir() {
				return nil
			}

			if filepath.Ext(file.Name()) == ".txt" {
				numberOfTxtFiles ++
			}
			return nil
		})

	go fileWalk(input, filePathChannel, stopFileWalkChannel)
	go textToPostgres(lineChannel, *copySize, *db, &stopToolChannel)
	go readFileStarter(compiledRegex, filePathChannel, &lineChannel, &currentGoroutinesChannel, numberOfTxtFiles, &numberOfProcessedFiles)

	log.Println("Waiting to close Filepath Channel")
	<- stopFileWalkChannel
	log.Println("Closing Filepath Channel")
	close(filePathChannel)

	for {
		// If I remove this line it stops working
		time.Sleep(1 * time.Second)

		if len(currentGoroutinesChannel) == 0 && len(lineChannel) == 0 {
			log.Println("CLOSING LINE CHANNEL")
			close(lineChannel)
			break
		}
	}
	<-stopToolChannel
}

func readFileStarter(delimiters *regexp.Regexp, filePathChannel chan string, lineChannel *chan string, currentGoroutinesChannel *chan int, numberOfTxtFiles int, numberOfProcessedFiles *int)  {
	for {
		path, morePaths := <-filePathChannel
		if morePaths {
			*currentGoroutinesChannel <- 1
			go readFile(path, delimiters, lineChannel, *currentGoroutinesChannel, numberOfTxtFiles, numberOfProcessedFiles)
		} else {
			log.Println("No more files to process")
			break
		}
	}

}

func readFile(path string, delimiters *regexp.Regexp, lineChannel *chan string, currentGoroutinesChannel chan int, numberOfTxtFiles int, numberOfProcessedFiles *int) {

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
			insert := delimiters.ReplaceAllString(line, "${1}:$2")
			*lineChannel <- insert
		}
	}
	*numberOfProcessedFiles ++
	log.Printf("Done reading %v / %v", *numberOfProcessedFiles, numberOfTxtFiles)
	<-currentGoroutinesChannel

}

func fileWalk(dataSource *string, filePathChannel chan string, stopFileWalkChannel chan bool) {
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
				// log.Printf("reading %s, %vB", path, file.Size())
				filePathChannel <- path
			}
			return nil
		})

	stopFileWalkChannel <- true
}

func textToPostgres(lineChannel chan string, copySize int, db sql.DB, stopToolChannel *chan bool) {

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
		line, more := <-lineChannel

		splitLine := strings.SplitN(line, ":", 2)

		if len(splitLine) == 2 {
			if utf8.Valid([]byte(splitLine[0])) && utf8.Valid([]byte(splitLine[1])) {
				lineCount++
				_, err = stmt.Exec(splitLine[0], splitLine[1])

				if lineCount % copySize == 0 {
					_, err = stmt.Exec()
					if err != nil {
						log.Fatal(err)
					}

					stmt, err = txn.Prepare(pq.CopyIn("pwned", "username", "password"))
					if err != nil {
						log.Fatal(err)
					}

					log.Printf("Inserted %v lines", lineCount)

				}

				if err != nil {
					log.Println("error:", splitLine[0], splitLine[1])
					log.Fatal(err)
				}
			}
		}

		if !more {
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
			log.Printf("Inserted %v lines", lineCount)
			break
		}
	}
	log.Printf("DONE, IMPORTED %v FILES", lineCount)
	*stopToolChannel <- true
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Println("Finished Import at", time.Now().Format("02-Jan-2006 15:04:05"))
	log.Printf("%s took %s", name, elapsed)
}
