package main

import (
	"context"
	"flag"
	"log"

	"github.com/ueux/distribuita/protocol"
)

var port int

func main() {
	flag.IntVar(&port, "port", 5000, "port to run the server on")
	flag.Parse()

	err := protocol.StartHTTP(context.Background(), port)
	log.Fatal(err)
}
