package kvDB

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const Path = "database"

type KvDB struct {
	idx      map[string]int64 // 索引的位置
	db       *DBOpen          // 数据文件
	filePath string           // 数据位置
	mu       sync.RWMutex     // 读写锁
}

func Open(filename string) (kv *KvDB, err error) {
	// 1. 判断目录文件是否存在，不存在创建文件
	if _, err = os.Stat(Path); os.IsNotExist(err) {
		if err = os.MkdirAll(Path, os.ModePerm); err != nil {
			return
		}
	}
	// 2. 加载文件
	var absPathFile string
	if absPathFile, err = filepath.Abs(Path); err != nil {
		return
	}
	absPathFilename := filepath.Join(absPathFile, filename)
	db, err := NewDBOpen(absPathFilename)
	if err != nil {
		return nil, err
	}

	kv = &KvDB{
		idx:      make(map[string]int64),
		db:       db,
		filePath: absPathFilename,
	}
	// 加载磁盘数据到内存
	kv.loadFromDisk()

	return kv, nil
}

func (k *KvDB) Put(key, value []byte) error {
	if len(key) == 0 {
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	offset := k.db.Offset
	entry := NewEntry(key, value, PUT)
	// 写入文件中
	err := k.db.Write(entry)
	// 写入 map 中
	k.idx[string(key)] = offset
	return err
}

func (k *KvDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}
	k.mu.RLock()
	defer k.mu.RUnlock()

	offset, err := k.exist(key)
	if err != nil {
		return
	}

	e, err := k.db.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e == nil {
		return nil, ErrKeyNotFound
	}
	return e.Value, nil

}

func (k *KvDB) exist(key []byte) (offset int64, err error) {
	offset, ok := k.idx[string(key)]
	if !ok {
		return 0, ErrKeyNotFound
	}
	return offset, nil
}

func (k *KvDB) loadFromDisk() {
	if k.db == nil {
		return
	}
	var offset int64
	for {
		e, err := k.db.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		// 记录索引位置
		k.idx[string(e.Key)] = offset
		// mark 标识
		if e.Mark == DEL {
			delete(k.idx, string(e.Key))
		}
		// 移动偏移量
		offset += e.Len()
	}
}

func (k *KvDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	_, err = k.exist(key)
	if errors.Is(err, ErrKeyNotFound) {
		err = nil
		return
	}
	// 封装成 Entry 并写入
	e := NewEntry(key, nil, DEL)
	err = k.db.Write(e)
	if err != nil {
		return
	}
	// 删除内存中的 key
	delete(k.idx, string(key))
	return
}

func (k *KvDB) Close() error {
	if k.db == nil {
		return ErrInvalidDBFile
	}
	return k.db.File.Close()
}
