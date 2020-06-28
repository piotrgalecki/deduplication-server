// Deduplication Server
// run server on port :8080
// handle PUT requests by saving received data excluding duplicate lines to the file specified by URL
// handle GET requests by sending the content of the request file to a client

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

const DedupSrvHome string = "/tmp/dedupSrvHome"

// handle GET request by sending the content of the request file to client
func ProcessGetRequest(w *http.ResponseWriter, r *http.Request) {
	log.Println("Received GET request ", r.URL.Path)

	// open the file
	file, err := os.Open(DedupSrvHome + r.URL.Path) // For read access.
	if err != nil {
		log.Fatalln("could not open file ", r.URL.Path, " err ", err)
		http.NotFound(*w, r)
		return
	}
	defer file.Close()

	// read
	var lines int = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		word := scanner.Text()
		lines++
		//log.Println(word)
		fmt.Fprintln(*w, word)
	}
	if serr := scanner.Err(); serr != nil {
		log.Fatalln("error reading the file", serr)
		return
	}
	log.Println("successfully processed GET request for ", r.URL.Path, "lines ", lines)
}

// handle PUT request by writing received data excluding duplicate lines to the file specified
func ProcessPutRequest(w *http.ResponseWriter, r *http.Request) {
	log.Println("Received PUT request", r.URL)

	// create a temp file
	file, err := ioutil.TempFile("/tmp", "putdata.*.txt")
	if err == nil {
		log.Println("created a temp file", file.Name())
	} else {
		log.Fatalln("could not create a temp file", err)
		http.Error(*w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// create a map to store words
	wordMap := make(map[string]bool)

	// read words from the payload
	var lines int = 0
	var duplicates int = 0
	scanner := bufio.NewScanner(r.Body)
	for scanner.Scan() {
		word := scanner.Text()
		if word == "" {
			continue
		}
		lines++
		//log.Println(word)

		// skip word if it is a duplicate
		if wordMap[word] == true {
			duplicates++
			//log.Println("skipping word ", word)
			continue
		}

		// mark word as encountered
		wordMap[word] = true

		// output word to the file
		n, err := file.WriteString(word + "\n")
		if err != nil {
			log.Fatalln("error processing PUT request", n, err)
			http.Error(*w, http.StatusText(http.StatusInsufficientStorage), http.StatusInsufficientStorage)
			defer os.Remove(file.Name())
			return
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalln("error processing PUT request", err)
		http.Error(*w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		defer os.Remove(file.Name())
		return
	}

	// move the temp file to requested path
	targetlocation := DedupSrvHome + r.URL.Path
	mverr := os.Rename(file.Name(), targetlocation)
	if mverr != nil {
		log.Fatalln("error moving temp file to target location", targetlocation)
		http.Error(*w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		defer os.Remove(file.Name())
		return
	}
	log.Println("successfully processed PUT request to ", targetlocation,
		" lines ", lines, "duplicates ", duplicates)
}

func HelloServer(w http.ResponseWriter, r *http.Request) {

	if r.Method == http.MethodGet {
		ProcessGetRequest(&w, r)
	} else if r.Method == http.MethodPut {
		ProcessPutRequest(&w, r)
	} else {
		log.Fatalln("Unsupported request method")
		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
	}
}

func main() {

	err := os.MkdirAll(DedupSrvHome, 0700)
	if err != nil {
		log.Fatalln("creating dedup server home failed", err)
		return
	}
	http.HandleFunc("/", HelloServer)
	http.ListenAndServe(":8080", nil)
}
