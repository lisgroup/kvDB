package kvDB

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type KvDB struct {
	idx      map[string]int64 // 索引的位置
	db       *DBOpen          // 数据文件
	filePath string           // 数据位置
	mu       sync.RWMutex     // 读写锁
}

func Open(dirPath string) (kv *KvDB, err error) {
	// 如果数据库目录不存在，则新建一个
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dirAbsPath, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, err
	}
	db, err := NewDBOpen(dirAbsPath)
	if err != nil {
		return nil, err
	}

	kv = &KvDB{
		idx:      make(map[string]int64),
		db:       db,
		filePath: dirAbsPath,
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

func (k *KvDB) Merge() error {
	if k.db.Offset == 0 {
		return nil
	}
	var validEntry []*Entry
	var offset int64
	for {
		e, err := k.db.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 判断是否是有效数据
		if e.Mark == PUT {
			validEntry = append(validEntry, e)
		}
		offset += e.Len()
	}
	// 重新写入磁盘
	file, err := NewMergeDBFile(k.filePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(file.File.Name())
	}()
	k.mu.Lock()
	defer k.mu.Unlock()
	return nil
}

func (k *KvDB) Close() error {
	if k.db == nil {
		return ErrInvalidDBFile
	}
	return k.db.File.Close()
}
