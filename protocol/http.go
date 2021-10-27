package protocol

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/ueux/distribuita/store"
)

func StartHTTP(ctx context.Context, port int) error {
	http.HandleFunc("/store", withStore(store.New(ctx)))

	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func withStore(s store.Store) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s := store.New(r.Context())
		storeHandler(s, w, r)
	}
}

func storeHandler(s store.Store, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		val, err := s.Get(q.Get("key"))
		if errors.Is(err, store.ErrKeyNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if errors.Is(err, store.ErrStoreClosed) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write(val)
	case http.MethodPost:
	case http.MethodDelete:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}
