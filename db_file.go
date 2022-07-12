package aliDB

import "os"

const (
	FileName = "minidb.data"
)

type DBFile struct {
	File   *os.File
	Offset int64
}

func newInternal(fileName string) (*DBFile, error) {

	//打开指定文件，没有则创建
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	//获取文件offset
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DBFile{
		File:   file,
		Offset: stat.Size(),
	}, nil
}

func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

//写一条数据到文件
func (db *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = db.File.WriteAt(enc, db.Offset)
	db.Offset += e.GetOneEntrySize()
	return
}

//读一条数据
func (db *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = db.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	//读取key
	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = db.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}
	//读取value
	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = db.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}

	return
}
