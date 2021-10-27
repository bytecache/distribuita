package store_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ueux/distribuita/store"
)

func TestSet(t *testing.T) {
	tt := []struct {
		Name          string
		Key           string
		Value         []byte
		ExpectedError error
		ExpectedValue []byte
	}{
		{
			Name:          "add non-existant key",
			Key:           "foo",
			Value:         []byte("bar"),
			ExpectedError: nil,
			ExpectedValue: []byte("bar"),
		},
		{
			Name:          "add key that already exists",
			Key:           "foo",
			Value:         []byte("bar"),
			ExpectedError: store.ErrKeyAlreadyExists,
			ExpectedValue: []byte("bar"),
		},
		{
			Name:          "error returned if store closed",
			Key:           "foo",
			ExpectedError: store.ErrStoreClosed,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := store.New(ctx)

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.ExpectedError == store.ErrStoreClosed {
				cancel()
				time.Sleep(time.Millisecond)
			}

			err := s.Set(tc.Key, tc.Value)
			if err != tc.ExpectedError {
				t.Fatalf("got error [%v], want error [%v]", err, tc.ExpectedError)
			}

			val, _ := s.Get(tc.Key)
			if !bytes.Equal(val, tc.ExpectedValue) {
				t.Fatalf("got error [%v], want error [%v]", val, tc.ExpectedValue)
			}
		})
	}

	cancel()
}

func TestGet(t *testing.T) {
	tt := []struct {
		Name          string
		Key           string
		ExpectedError error
		ExpectedValue []byte
	}{
		{
			Name:          "test get for key that exists",
			Key:           "foo",
			ExpectedError: nil,
			ExpectedValue: []byte("bar"),
		},
		{
			Name:          "test get for key that does not exist",
			Key:           "baz",
			ExpectedError: store.ErrKeyNotFound,
			ExpectedValue: nil,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := store.New(ctx)
	s.Set("foo", []byte("bar"))

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.ExpectedError == store.ErrStoreClosed {
				cancel()
				time.Sleep(time.Millisecond)
			}

			val, err := s.Get(tc.Key)
			if err != tc.ExpectedError {
				t.Fatalf("got error [%v], want error [%v]", err, tc.ExpectedError)
			}

			if !bytes.Equal(val, tc.ExpectedValue) {
				t.Fatalf("got value [%v], want value [%v]", val, tc.ExpectedValue)
			}
		})
	}

	cancel()
}

func TestUpdate(t *testing.T) {
	tt := []struct {
		Name          string
		Key           string
		Value         []byte
		ExpectedError error
		ExpectedValue []byte
	}{
		{
			Name:          "update for key that exists",
			Key:           "foo",
			Value:         []byte("baz"),
			ExpectedError: nil,
			ExpectedValue: []byte("baz"),
		},
		{
			Name:          "update for key that does not exist",
			Key:           "baz",
			ExpectedError: store.ErrKeyNotFound,
			ExpectedValue: nil,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := store.New(ctx)
	s.Set("foo", []byte("bar"))

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.ExpectedError == store.ErrStoreClosed {
				cancel()
				time.Sleep(time.Millisecond)
			}

			err := s.Update(tc.Key, tc.Value)
			if err != tc.ExpectedError {
				t.Fatalf("got error [%v], want error [%v]", err, tc.ExpectedError)
			}

			val, _ := s.Get(tc.Key)
			if !bytes.Equal(val, tc.Value) {
				t.Fatalf("got value [%v], want value [%v]", val, tc.Value)
			}
		})
	}

	cancel()
}

func TestDelete(t *testing.T) {
	tt := []struct {
		Name          string
		Key           string
		Value         []byte
		ExpectedError error
	}{
		{
			Name:          "delete for key that exists",
			Key:           "foo",
			ExpectedError: nil,
		},
		{
			Name:          "delete for key that does not exist",
			Key:           "baz",
			ExpectedError: store.ErrKeyNotFound,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := store.New(ctx)
	s.Set("foo", []byte("bar"))

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.ExpectedError == store.ErrStoreClosed {
				cancel()
				time.Sleep(time.Millisecond)
			}

			err := s.Delete(tc.Key)
			if err != tc.ExpectedError {
				t.Fatalf("got error [%v], want error [%v]", err, tc.ExpectedError)
			}
		})
	}

	cancel()
}
