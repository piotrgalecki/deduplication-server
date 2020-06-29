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

const DedupSrvHome string = "/tmp/dedupSrvHome/"
const DedupSrvCache string = "/tmp/dedupdiskcache/"

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
	tempFile, err := ioutil.TempFile("/tmp", "putdata.*.txt")
	if err != nil {
		log.Fatalln("could not create a temp file", err)
		http.Error(*w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	log.Println("created a temp file ", tempFile.Name())

	// initialize disk cache
	mkerr := os.MkdirAll(DedupSrvCache+tempFile.Name(), 0700)
	if mkerr != nil {
		log.Fatalln("creating dedup server disk cache failed", err)
		http.Error(*w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

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
		var wordpath string = DedupSrvCache + tempFile.Name() + "/" + word
		ofd, oerr := os.Open(wordpath)
		if oerr == nil {
			duplicates++
			//log.Println("skipping word ", word)
			ofd.Close()
			continue
		}

		// mark word as encountered by creating a file in diskcache
		cfd, cerr := os.Create(wordpath)
		if cerr != nil {
			log.Fatalln("error creating a word marker file in cache", cfd, cerr)
			http.Error(*w, http.StatusText(http.StatusInsufficientStorage), http.StatusInsufficientStorage)
			defer os.Remove(tempFile.Name())
			defer os.RemoveAll(DedupSrvCache + tempFile.Name())
			return
		}
		cfd.Close()

		// output word to the file
		n, err := tempFile.WriteString(word + "\n")
		if err != nil {
			log.Fatalln("error processing PUT request", n, err)
			http.Error(*w, http.StatusText(http.StatusInsufficientStorage), http.StatusInsufficientStorage)
			defer os.Remove(tempFile.Name())
			defer os.RemoveAll(DedupSrvCache + tempFile.Name())
			return
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalln("error processing PUT request", err)
		http.Error(*w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		defer os.Remove(tempFile.Name())
		defer os.RemoveAll(DedupSrvCache + tempFile.Name())
		return
	}

	// move the temp file to requested path
	targetlocation := DedupSrvHome + r.URL.Path
	mverr := os.Rename(tempFile.Name(), targetlocation)
	if mverr != nil {
		log.Fatalln("error moving temp file to target location", targetlocation)
		http.Error(*w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		defer os.Remove(tempFile.Name())
		defer os.RemoveAll(DedupSrvCache + tempFile.Name())
		return
	}

	// purge disk cache
	defer os.RemoveAll(DedupSrvCache + tempFile.Name())

	log.Println("successfully processed PUT request to ", targetlocation,
		" lines ", lines, "duplicates ", duplicates)
}

func HelloServer(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case http.MethodGet:
		ProcessGetRequest(&w, r)
	case http.MethodPut:
		ProcessPutRequest(&w, r)
	default:
		log.Fatalln("Unsupported request method")
		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
	}
}

func main() {

	// create deduplication server home dir
	err := os.MkdirAll(DedupSrvHome, 0700)
	if err != nil {
		log.Fatalln("creating dedup server home failed", err)
		return
	}

	http.HandleFunc("/", HelloServer)
	http.ListenAndServe(":8080", nil)
}
