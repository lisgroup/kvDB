package benchmark

import (
	"bytes"
	"encoding/binary"
	"github.com/lisgroup/kvDB"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

var kv *kvDB.KvDB

func init() {
	// 初始化数据库
	var err error
	kv, err = kvDB.Open(kvDB.Path)
	if err != nil {
		panic(err)
	}
}

func BenchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 256; i < b.N; i++ {
		// 创建一个字节缓冲区
		buf := new(bytes.Buffer)
		// 将 int 值写入缓冲区，使用小端字节序
		_ = binary.Write(buf, binary.LittleEndian, int64(i))
		k := buf.Bytes()
		err := kv.Put(k, k)
		assert.Nil(b, err)
	}
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	for i := 256; i < b.N; i++ {
		// 创建一个字节缓冲区
		buf := new(bytes.Buffer)
		// 将 int 值写入缓冲区，使用小端字节序
		_ = binary.Write(buf, binary.LittleEndian, int64(i))
		k := buf.Bytes()
		val, err := kv.Get(k)
		if err != nil {
			log.Println(err)
			return
		}
		if string(val) != string(k) {
			panic("value not equal")
		}
	}
}
