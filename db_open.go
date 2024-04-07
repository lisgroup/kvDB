package kvDB

import "os"

type DBOpen struct {
	File   *os.File
	Offset int64
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

func NewDBOpen(absPathFilename string) (*DBOpen, error) {
	file, err := os.OpenFile(absPathFilename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(absPathFilename)
	if err != nil {
		return nil, err
	}

	return &DBOpen{
		File:   file,
		Offset: stat.Size(),
	}, nil
}
