package kvDB

import (
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

	return &KvDB{
		idx:      make(map[string]int64),
		db:       db,
		filePath: absPathFilename,
	}, nil
}
