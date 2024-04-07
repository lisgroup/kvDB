package kvDB

import "testing"

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
