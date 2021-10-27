package store

import (
	"context"
	"errors"
)

// Common store errors when interacting with CRUD operations.
var (
	ErrKeyNotFound      = errors.New("key not found in store")
	ErrKeyAlreadyExists = errors.New("key is already in use")
	ErrStoreClosed      = errors.New("store has been closed")
)

type Store struct {
	items   map[string][]byte
	mailbox chan interface{}
	done    chan struct{}
}

type keyRequest struct {
	key      string
	response chan response
}

type keyValueRequest struct {
	keyRequest
	value []byte
}

type response struct {
	value []byte
	err   error
}

type (
	getAction    struct{ keyRequest }
	setAction    struct{ keyValueRequest }
	updateAction struct{ keyValueRequest }
	deleteAction struct{ keyRequest }
)

// New initialises the store structure enabling concurrent read and
// write access to it. The passed in context can be be used to tear
// down the store.
func New(ctx context.Context) Store {
	s := Store{
		items:   make(map[string][]byte),
		mailbox: make(chan interface{}),
		done:    make(chan struct{}),
	}

	go s.startActor(ctx)

	return s
}

// Set will create a new item in the store if it does not already
// exist. If the key already exists, or the store has been closed
// an error will be returned to the caller.
func (s *Store) Set(key string, value []byte) error {
	if s.closed() {
		return ErrStoreClosed
	}

	_, ok := s.items[key]
	if ok {
		return ErrKeyAlreadyExists
	}

	s.items[key] = value

	return nil
}

// Get will retrieve an item from the store if it exists,
// returning an error if it does not or if the store has
// been closed previously.
func (s Store) Get(key string) ([]byte, error) {
	if s.closed() {
		return nil, ErrStoreClosed
	}

	v, ok := s.items[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

// Update will mutate an existing key, value pair. If the key
// does not already exist or the store has been closed an
// error will be returned to the caller.
func (s *Store) Update(key string, value []byte) error {
	if s.closed() {
		return ErrStoreClosed
	}

	_, ok := s.items[key]
	if !ok {
		return ErrKeyNotFound
	}

	s.items[key] = value

	return nil
}

// Delete will attempt to delete a key, returning an error if
// either the store is closed or the key does not exist.
func (s *Store) Delete(key string) error {
	if s.closed() {
		return ErrStoreClosed
	}

	_, ok := s.items[key]
	if !ok {
		return ErrKeyNotFound
	}

	delete(s.items, key)

	return nil
}

func (s *Store) startActor(ctx context.Context) {
	go func() {
		<-ctx.Done()
		close(s.mailbox)
	}()

	for msg := range s.mailbox {
		switch m := msg.(type) {
		case getAction:
			value, err := s.Get(m.key)
			m.response <- response{value, err}

		case setAction:
			m.response <- response{err: s.Set(m.key, m.value)}

		case updateAction:
			m.response <- response{err: s.Update(m.key, m.value)}

		case deleteAction:
			m.response <- response{err: s.Delete(m.key)}
		}
	}

	close(s.done)
}

func (s Store) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}
