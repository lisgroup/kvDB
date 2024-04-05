package kvDB

import "testing"

func TestOpen(t *testing.T) {
	kv, err := Open("./kv.data")
	if err != nil {
		t.Error(err)
	}
	t.Log(kv)
}
