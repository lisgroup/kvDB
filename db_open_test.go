package kvDB

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	kv, err := Open("kv.db")
	if err != nil {
		t.Fatal(err)
	}

	// 测试Put方法
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
	kv, err := Open("kv.db")
	if err != nil {
		t.Fatal(err)
	}

	// 测试Get方法
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
	kv, err := Open("kv.db")
	if err != nil {
		t.Fatal(err)
	}
	// 测试Put方法
	err = kv.Put([]byte("key1"), []byte("value1"))
	// 测试Get方法
	val, err := kv.Get([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
	t.Log(string(val))

	// 测试Del方法
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
