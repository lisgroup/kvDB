package kvDB

import (
	"os"
	"sync"
)

type DBOpen struct {
	File    *os.File
	Offset  int64
	BufPool *sync.Pool
}

func NewDBOpen(absPathFilename string) (*DBOpen, error) {
	file, err := os.OpenFile(absPathFilename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(absPathFilename)
	if err != nil {
		return nil, err
	}

	pool := &sync.Pool{New: func() interface{} {
		return make([]byte, entryHeaderSize)
	}}
	return &DBOpen{
		File:    file,
		Offset:  stat.Size(),
		BufPool: pool,
	}, nil
}

func (o *DBOpen) Write(entry *Entry) error {
	// 写入文件前先编码
	data, err := entry.Encode()
	if err != nil {
		return err
	}
	// 写入文件
	_, err = o.File.WriteAt(data, o.Offset)
	o.Offset += entry.Len()
	return err
}

func (o *DBOpen) Read(offset int64) (e *Entry, err error) {
	buf := o.BufPool.Get().([]byte)
	defer o.BufPool.Put(buf)
	// buf := make([]byte, entryHeaderSize)
	if _, err = o.File.ReadAt(buf, offset); err != nil {
		return
	}
	e = &Entry{}
	e.Decode(buf)
	offset += entryHeaderSize
	if e.KeyLen > 0 {
		key := make([]byte, e.KeyLen)
		if _, err = o.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}
	offset += int64(e.KeyLen)
	if e.ValueLen > 0 {
		value := make([]byte, e.ValueLen)
		if _, err = o.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}
