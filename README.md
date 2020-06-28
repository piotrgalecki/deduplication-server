# deduplication-server

// HTTP server writen in Golang
// it runs on port :8080
// handle PUT requests by saving received data excluding duplicate lines to the file specified by URL
// handle GET requests by sending the content of the request file to a client
