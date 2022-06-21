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
	mailbox chan request
	done    chan struct{}
}

type request struct {
	action   string
	key      string
	value    []byte
	response chan response
}

type response struct {
	value []byte
	err   error
}

// New initialises the store structure enabling concurrent read and
// write access to it. The passed in context can be be used to tear
// down the store.
func New(ctx context.Context) Store {
	s := Store{
		items:   make(map[string][]byte),
		mailbox: make(chan request),
		done:    make(chan struct{}),
	}

	go s.startActor(ctx)

	return s
}

// Set will create a new item in the store if it does not already
// exist. If the key already exists, or the store has been closed
// an error will be returned to the caller.
func (s *Store) Set(key string, value []byte) error {
	resp := make(chan response)
	select {
	case <-s.done:
		return ErrStoreClosed
	default:
		s.mailbox <- request{
			action:   "set",
			key:      key,
			value:    value,
			response: resp,
		}
	}

	result := <-resp

	return result.err
}

// Get will retrieve an item from the store if it exists,
// returning an error if it does not or if the store has
// been closed previously.
func (s *Store) Get(key string) ([]byte, error) {
	resp := make(chan response)
	select {
	case <-s.done:
		return nil, ErrStoreClosed
	default:
		s.mailbox <- request{
			action:   "get",
			key:      key,
			response: resp,
		}
	}

	result := <-resp

	return result.value, result.err
}

// Update will mutate an existing key, value pair. If the key
// does not already exist or the store has been closed an
// error will be returned to the caller.
func (s *Store) Update(key string, value []byte) error {
	resp := make(chan response)
	select {
	case <-s.done:
		return ErrStoreClosed
	default:
		s.mailbox <- request{
			action:   "update",
			key:      key,
			value:    value,
			response: resp,
		}
	}

	result := <-resp

	return result.err
}

// Delete will attempt to delete a key, returning an error if
// either the store is closed or the key does not exist.
func (s *Store) Delete(key string) error {
	resp := make(chan response)
	select {
	case <-s.done:
		return ErrStoreClosed
	default:
		s.mailbox <- request{
			action:   "delete",
			key:      key,
			response: resp,
		}
	}

	result := <-resp

	return result.err
}

func (s *Store) startActor(ctx context.Context) {
	go func() {
		<-ctx.Done()
		close(s.mailbox)
	}()

	for msg := range s.mailbox {
		switch msg.action {
		case "get":
			v, ok := s.items[msg.key]
			if !ok {
				msg.response <- response{err: ErrKeyNotFound}
				continue
			}
			msg.response <- response{value: v}
		case "set":
			_, ok := s.items[msg.key]
			if ok {
				msg.response <- response{err: ErrKeyAlreadyExists}
				continue
			}
			s.items[msg.key] = msg.value
			msg.response <- response{}
		case "update":
			_, ok := s.items[msg.key]
			if !ok {
				msg.response <- response{err: ErrKeyNotFound}
				continue
			}
			s.items[msg.key] = msg.value
			msg.response <- response{}
		case "delete":
			_, ok := s.items[msg.key]
			if !ok {
				msg.response <- response{err: ErrKeyNotFound}
				continue
			}
			delete(s.items, msg.key)
			msg.response <- response{}
		}
	}

	close(s.done)
}
