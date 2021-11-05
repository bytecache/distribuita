package protocol

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/ueux/distribuita/store"
)

func StartHTTP(ctx context.Context, port int) error {
	s := store.New(ctx)
	http.HandleFunc("/store", withStore(ctx, s))
	http.HandleFunc("/health", healthHandler)

	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func withStore(ctx context.Context, s store.Store) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		r.WithContext(ctx)
		storeHandler(s, w, r)
	}
}

func storeHandler(s store.Store, w http.ResponseWriter, r *http.Request) {
	log.Printf("serving request method[%s] query[%v]", r.Method, r.URL.RawQuery)
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
	case http.MethodPut:
		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		key := q.Get("key")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = s.Set(key, b)
		if err == nil {
			w.WriteHeader(http.StatusCreated)
			return
		}

		if errors.Is(err, store.ErrStoreClosed) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = s.Update(key, b)
		if err == nil {
			w.WriteHeader(http.StatusOK)
			return
		}

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	case http.MethodDelete:
		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		key := q.Get("key")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = s.Delete(key)
		if errors.Is(err, store.ErrStoreClosed) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if errors.Is(err, store.ErrKeyNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
