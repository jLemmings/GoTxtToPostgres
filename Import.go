package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
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

type credential struct {
	username      string
	clearPassword string
	md5Password   string
	sha1Password  string
}

var (
	query               string
	err                 error
	table				string
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

	credChannel := make(chan credential, 1000)
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
		log.Println("Failed")
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
	go textToPostgres(credChannel, *copySize, db, stopToolChannel)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go readFile(filePathChannel, compiledRegex, credChannel, numberOfTxtFiles, &numberOfProcessedFiles, &wg, hashesMap)
	}

	log.Println("Waiting to close Filepath Channel")
	<-stopFileWalkChannel
	log.Println("Closing Filepath Channel")
	close(filePathChannel)

	log.Println("WAITING")
	wg.Wait()
	log.Println("CLOSING LINE CHANNEL")
	close(credChannel)

	<-stopToolChannel
}

func readFile(filePathChannel chan string, delimiters *regexp.Regexp, credChannel chan credential, numberOfTxtFiles int, numberOfProcessedFiles *int, wg *sync.WaitGroup, hashesMap map[string]*regexp.Regexp) {
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
					strings.Replace(line, "\u0000", "", -1)
					insert := delimiters.ReplaceAllString(line, "${1}:$2")
					splitLine := strings.SplitN(insert, ":", 2)

					credentialForChan := credential{}

					if len(splitLine) == 2 && utf8.ValidString(splitLine[0]) && utf8.ValidString(splitLine[1]) {

						username := string(splitLine[0])
						password := string(splitLine[1])

						if hashesMap["MD5"].Match([]byte(password)) {
							credentialForChan = credential{username: username, md5Password: password}
						} else if hashesMap["SHA1"].Match([]byte(password)) {
							credentialForChan = credential{username: username, sha1Password: password}
						} else {
							credentialForChan = credential{username: username, clearPassword: password}
						}
					}
					credChannel <- credentialForChan
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

func textToPostgres(credChannel chan credential, copySize int, db *sql.DB, stopToolChannel chan bool) {
	defer func() {
		err = db.Close()
		handleErr(err)

		stopToolChannel <- true
	}()

	log.Println("Started Text to postgres goroutine")
	var lineCount int64 = 0

	print(query)

	for {
		credentialToInsert, more := <-credChannel

		if !more {
			log.Printf("Inserted %v lines", lineCount)
			break
		}

		var password string

		switch {
		case credentialToInsert.clearPassword != "":
			table = "clear"
			password = credentialToInsert.clearPassword
		case credentialToInsert.sha1Password != "":
			table = "sha1"
			password = credentialToInsert.sha1Password
		case credentialToInsert.md5Password != "":
			table = "md5"
			password = credentialToInsert.md5Password
		}


		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", table), credentialToInsert.username, password)

		handleErr(err)

		lineCount++

		if lineCount%int64(copySize*10) == 0 {

			log.Printf("Inserted %v lines", lineCount)

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

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Println("Finished Import at", time.Now().Format("02-Jan-2006 15:04:05"))
	log.Printf("%s took %s", name, elapsed)
}
