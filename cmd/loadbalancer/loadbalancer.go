package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"github.com/ueux/conhash"
)

var (
	ports string
	nodes []conhash.Node
)

func main() {
	flag.StringVar(&ports, "ports", "5000,5001,5002", "comma separated list of ports")
	flag.Parse()

	ps := strings.Split(ports, ",")
	for _, p := range ps {
		port, err := strconv.Atoi(p)
		if err != nil {
			panic(err)
		}

		node := conhash.Node{
			Host: "localhost",
			Port: port,
		}

		nodes = append(nodes, node)
	}

	r := conhash.New(nodes)

	http.HandleFunc("/store", withRing(r))

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func withRing(chr conhash.Ring) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := balanceHandler(chr, w, r)
		proxy.ServeHTTP(w, r)
	}
}

func balanceHandler(chr conhash.Ring, w http.ResponseWriter, r *http.Request) *httputil.ReverseProxy {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return nil
	}

	key := q.Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return nil
	}

	node := chr.Find(key)

	target, err := url.Parse(fmt.Sprintf("http://%s:%d", node.Host, node.Port))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return nil
	}

	return httputil.NewSingleHostReverseProxy(target)
}
