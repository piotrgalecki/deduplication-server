package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func putRequest(url string, data io.Reader) (err error) {
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPut, url, data)
	if err != nil {
		// handle error
		fmt.Println(err)
		return err
	}
	_, err = client.Do(req)
	if err != nil {
		// handle error
		fmt.Println(err)
		return err
	}
	return nil
}

func main() {

	// create a test string with 10k unique words and 5k duplicate words
	var data string
	var i int = 0
	var words int = 10000
	var duplicates int = 5000
	for i < words {
		data += "word" + strconv.Itoa(i) + "\n"
		i++
	}
	for i = 0; i < duplicates; {
		data += "word" + strconv.Itoa(i) + "\n"
		i++
	}

	// send test data to server for processing
	var url string = "http://localhost:8080/unittest.txt"
	err := putRequest(url, strings.NewReader(data))
	if err != nil {
		fmt.Println("TEST FAILED", err)
		return
	}

	// get processed file from server
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("TEST FAILED", err)
		return
	}
	defer resp.Body.Close()

	// verify that processed file has correct number of lines
	scanner := bufio.NewScanner(resp.Body)
	for i = 0; scanner.Scan(); {
		word := scanner.Text()
		if word == "" {
			continue
		}
		i++
	}

	if i != words {
		fmt.Println("TEST FAILED expected word count ", words, " actual word count ", i)
		return
	}
	fmt.Println("TEST PASSED expected word count ", words, " actual word count ", i)
}
