package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ueux/conhash"
)

var (
	ports string
	nodes []conhash.Node
)

func main() {
	flag.StringVar(&ports, "ports", "6000,6001,6002,6003", "comma separated list of ports")
	flag.Parse()

	ch := make(chan conhash.Node)
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
		go healthChecker(node, ch)
	}

	r := conhash.New(nodes)
	go stateWorker(&r, ch)

	http.HandleFunc("/store", withRing(&r))

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func withRing(chr *conhash.Ring) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := balanceHandler(chr, w, r)
		proxy.ServeHTTP(w, r)
	}
}

func balanceHandler(chr *conhash.Ring, w http.ResponseWriter, r *http.Request) *httputil.ReverseProxy {
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

func healthChecker(node conhash.Node, stateCh chan conhash.Node) {
	client := http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s:%d/health", node.Host, node.Port), nil)

	down := false

	t := time.NewTicker(5 * time.Second)
	for range t.C {
		resp, err := client.Do(req)
		if err != nil {
			if !down {
				log.Printf("node[%s:%d] - down (removing from cluster)", node.Host, node.Port)
				stateCh <- node
				down = true
			}
			continue
		}

		if resp.StatusCode == http.StatusOK && down {
			log.Printf("node[%s:%d] - up (adding to cluster)", node.Host, node.Port)
			stateCh <- node
			down = false
		}

		resp.Body.Close()
	}
}

func stateWorker(r *conhash.Ring, nodeState chan conhash.Node) {
	for node := range nodeState {
		err := r.Remove(node)
		if errors.Is(err, conhash.ErrNodeNotFound) {
			r.Add(node)
		}
	}
}
