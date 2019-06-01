package main

import (
	"database/sql"
	"flag"
	"github.com/gorilla/mux"
	"github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

func main() {
	// Flags
	input := flag.String("input", "/Users/joshuahemmings/Documents/Dev/Personal/GoTxtToPostgres/testDocuments", "Data to Import [STRING]")
	delimiters := flag.String("delimiters", ";:|", "delimiters list [STRING]")
	concurrency := flag.Int("concurrency", 10, "Concurrency (amount of GoRoutines) [INT]")
	copySize := flag.Int("copySize", 2, "How many rows get imported per execution [INT]")
	dbUser := flag.String("dbUser", "pwned", "define DB username")
	dbName := flag.String("dbName", "pwned", "define DB name")
	// dbTable := flag.String("dbTable", "", "define DB table")
	dbPassword := flag.String("dbPassword", "123", "define DB password")
	dbHost := flag.String("dbHost", "192.168.178.206", "define DB host")
	flag.Parse()

	compiledRegex := regexp.MustCompile("^(.*?)[" + *delimiters + "](.*)$")

	md5Regex := regexp.MustCompile("^[a-f0-9]{32}$")
	sha1Regex := regexp.MustCompile("\b[0-9a-f]{5,40}\b")

	var hashesMap map[string]*regexp.Regexp
	hashesMap = make(map[string]*regexp.Regexp)

	hashesMap["MD5"] = md5Regex
	hashesMap["SHA1"] = sha1Regex

	var wg = sync.WaitGroup{}

	lineChannel := make(chan string, 1000)
	filePathChannel := make(chan string, *concurrency*4)
	stopToolChannel := make(chan bool, 1)
	stopFileWalkChannel := make(chan bool, 1)

	numberOfTxtFiles := 0
	numberOfProcessedFiles := 0

	// TODO: Remove for stable version
	go func() {
		// Create a new router
		router := mux.NewRouter()

		// Register pprof handlers
		router.HandleFunc("/debug/pprof/", pprof.Index)
		router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		router.HandleFunc("/debug/pprof/profile", pprof.Profile)
		router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

		router.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		router.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		router.Handle("/debug/pprof/block", pprof.Handler("block"))
		router.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
		log.Fatal(http.ListenAndServe(":80", router))
	}()

	connStr := "host=" + *dbHost + " user=" + *dbUser + " dbname=" + *dbName + " password=" + *dbPassword + " sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	checkConnectionAndCreateTables(db)

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
				numberOfTxtFiles++
			}
			return nil
		})

	go fileWalk(input, filePathChannel, stopFileWalkChannel)
	go textToPostgres(lineChannel, *copySize, db, stopToolChannel, hashesMap)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go readFile(filePathChannel, compiledRegex, lineChannel, numberOfTxtFiles, &numberOfProcessedFiles, &wg)
	}

	log.Println("Waiting to close Filepath Channel")
	<-stopFileWalkChannel
	log.Println("Closing Filepath Channel")
	close(filePathChannel)

	log.Println("WAITING")
	wg.Wait()
	log.Println("CLOSING LINE CHANNEL")
	close(lineChannel)

	<-stopToolChannel
}

func readFile(filePathChannel chan string, delimiters *regexp.Regexp, lineChannel chan string, numberOfTxtFiles int, numberOfProcessedFiles *int, wg *sync.WaitGroup) {
	for {
		time.Sleep(5 * time.Second)
		path, morePaths := <-filePathChannel
		if morePaths {
			fileData, err := ioutil.ReadFile(path)
			if err != nil {
				log.Fatalf("Cannot read file %s", path)
				return
			}
			fileAsString := string(fileData)
			lines := strings.Split(fileAsString, "\n")

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					insert := delimiters.ReplaceAllString(line, "${1}:$2")
					lineChannel <- insert
				}
			}

			*numberOfProcessedFiles++
			log.Printf("Read %v / %v Files", *numberOfProcessedFiles, numberOfTxtFiles)
		} else {
			log.Println("Closing readFile Goroutine")
			break
		}
	}
	wg.Done()
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

	log.Println("stop file walk channel")
	stopFileWalkChannel <- true
}

func textToPostgres(lineChannel chan string, copySize int, db *sql.DB, stopToolChannel chan bool, hashesMap map[string]*regexp.Regexp) {

	log.Println("Started Text to postgres goroutine")
	var lineCount int64 = 0

	txnClear, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	txnMD5, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	txnSHA1, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	var txnMap map[string]*sql.Tx
	txnMap = make(map[string]*sql.Tx)

	txnMap["clear"] = txnClear
	txnMap["md5"] = txnMD5
	txnMap["sha1"] = txnSHA1

	clearStatement, err := txnClear.Prepare(pq.CopyIn("clear", "username", "password"))
	md5Statement, err := txnMD5.Prepare(pq.CopyIn("md5", "username", "password"))
	sha1Statement, err := txnSHA1.Prepare(pq.CopyIn("sha1", "username", "password"))

	var stmtMap map[string]*sql.Stmt
	stmtMap = make(map[string]*sql.Stmt)

	stmtMap["clear"] = clearStatement
	stmtMap["md5"] = md5Statement
	stmtMap["sha1"] = sha1Statement

	if err != nil {
		log.Fatal(err)
	}

	for {
		line, more := <-lineChannel
		strings.Replace(line, "\u0000", "", -1)
		splitLine := strings.SplitN(line, ":", 2)

		log.Println("GOT HERE")


		if len(splitLine) == 2 && utf8.ValidString(splitLine[0]) && utf8.ValidString(splitLine[1]) {

			username := string(splitLine[0])
			password := string(splitLine[1])

			if hashesMap["MD5"].Match([]byte(password)) {
				insertToDb(&lineCount, copySize, md5Statement, txnMD5, db, username, password)
			} else if hashesMap["SHA1"].Match([]byte(password)) {
				insertToDb(&lineCount, copySize, sha1Statement, txnMD5, db, username, password)
			} else {
				insertToDb(&lineCount, copySize, clearStatement, txnMD5, db, username, password)
			}
		}

		if !more {
			closeAllDbConnections(stmtMap, txnMap)

			log.Printf("Inserted %v lines", lineCount)
			break
		}
	}
	stopToolChannel <- true
}

func insertToDb(lineCount *int64, copySize int, statement *sql.Stmt, txn *sql.Tx, db *sql.DB, username string, password string) {
	*lineCount++
	_, err := statement.Exec(username, password)
	log.Println(*lineCount)

	if *lineCount%int64(copySize) == 0 {

		_, err = statement.Exec()
		if err != nil {
			log.Fatal("Failed at stmt.Exec", err)
		}

		err = statement.Close()
		if err != nil {
			log.Fatal("Failed at stmt.Close", err)
		}

		err = txn.Commit()
		if err != nil {
			log.Fatal("failed at txn.Commit", err)
		}

		log.Println("Got here")

		txn, err = db.Begin()
		if err != nil {
			log.Fatal("failed at db.Begin", err)
		}

		if *lineCount%(int64(copySize)*10) == 0 {
			log.Printf("Inserted %v lines", lineCount)
		}
	}

	if err != nil {
		log.Fatal(err, "source:", username, password)
	}
}

func closeAllDbConnections(stmtMap map[string]*sql.Stmt, txnMap map[string]*sql.Tx) {
	for k, v := range stmtMap {
		log.Printf("Closing %s Statement", k)
		_, err := v.Exec()
		err = v.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	for k, v := range txnMap {
		log.Printf("Closing %s Tx", k)
		err := v.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}

}

func checkConnectionAndCreateTables(db *sql.DB) {
	const queryClear = `CREATE TABLE IF NOT EXISTS clear (username varchar, password varchar)`
	const queryMD5 = `CREATE TABLE IF NOT EXISTS md5 (username varchar, password varchar)`
	const querySHA1 = `CREATE TABLE IF NOT EXISTS sha1 (username varchar, password varchar)`

	var version string
	serverVersion := db.QueryRow("SHOW server_version").Scan(&version)
	if serverVersion != nil {
		log.Fatal(serverVersion)
	}
	log.Println("Connected to:", version)

	_, errClear := db.Exec(queryClear)
	if errClear != nil {
		log.Fatal("Failed to create table if exists")
	}
	_, errMD5 := db.Exec(queryMD5)
	if errMD5 != nil {
		log.Fatal("Failed to create table if exists")
	}
	_, errSHA1 := db.Exec(querySHA1)
	if errSHA1 != nil {
		log.Fatal("Failed to create table if exists")
	}
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Println("Finished Import at", time.Now().Format("02-Jan-2006 15:04:05"))
	log.Printf("%s took %s", name, elapsed)
}
