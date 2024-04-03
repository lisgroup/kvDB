package kvDB

import "os"

type DBOpen struct {
	File   *os.File
	Offset int64
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
