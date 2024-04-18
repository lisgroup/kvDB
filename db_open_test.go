package kvDB

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	kv, err := Open(Path)
	if err != nil {
		t.Fatal(err)
	}

	// test Put method
	err = kv.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Error(err)
	}
	err = kv.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Error(err)
	}
}

func TestKvDB_Get(t *testing.T) {
	kv, err := Open(Path)
	if err != nil {
		t.Fatal(err)
	}

	// test Put method
	val, err := kv.Get([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
	t.Log(string(val))

	val, err = kv.Get([]byte("key2"))
	if err != nil {
		t.Error(err)
	}
	t.Log(string(val))
}

func TestKvDB_Del(t *testing.T) {
	kv, err := Open(Path)
	if err != nil {
		t.Fatal(err)
	}
	// test Put method
	err = kv.Put([]byte("key1"), []byte("value1"))
	// test Get method
	val, err := kv.Get([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
	t.Log(string(val))

	// test Del method
	err = kv.Del([]byte("key1"))
	if err != nil {
		t.Error(err)
	}

	val, err = kv.Get([]byte("key1"))
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		t.Error(err)
	}
	t.Log(string(val))
}
