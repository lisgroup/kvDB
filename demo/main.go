package main

import (
	"errors"
	"fmt"
	"github.com/lisgroup/kvDB"
)

func main() {
	// open kvDB
	kv, err := kvDB.Open(kvDB.Path)
	if err != nil {
		fmt.Println(err)
		return
	}
	// test Put method
	err = kv.Put([]byte("key1"), []byte("value1"))
	// test Get method
	val, err := kv.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("key1: %s\n", string(val))

	// test Del method
	// err = kv.Del([]byte("key1"))
	// if err != nil {
	// 	panic(err)
	// }

	val, err = kv.Get([]byte("key1"))
	if err != nil && !errors.Is(err, kvDB.ErrKeyNotFound) {
		panic(err)
	}
	fmt.Printf("key1: %s\n", string(val))
	// merge
	_ = kv.Merge()

	err = kv.Close()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("kvDB closed\n")
}
