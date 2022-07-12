package aliDB

import (
	"io"
	"os"
	"sync"
)

type AliDB struct {
	mu      sync.RWMutex
	df      *DBFile          //对应的数据文件
	fileDir string           //文件目录
	indexes map[string]int64 //索引
}

//开启一个数据库实例
func Open(fileDir string) (*AliDB, error) {

	// 如果数据库目录不存在，则新建一个
	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		if err := os.MkdirAll(fileDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dbFile, err := NewDBFile(fileDir)
	if err != nil {
		return nil, err
	}

	db := &AliDB{
		df:      dbFile,
		indexes: make(map[string]int64),
		fileDir: fileDir,
	}

	// 加载索引
	db.loadIndexesFromFile()
	return db, nil
}

//从文件加索引
func (db *AliDB) loadIndexesFromFile() {
	if db.df == nil {
		return
	}

	var offset int64

	for {
		e, err := db.df.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}
		//设置索引
		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			// 删除内存中的 key
			delete(db.indexes, string(e.Key))
		}
		offset += e.GetOneEntrySize()
	}
}

func (db *AliDB) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.df.Offset
	// 封装成 Entry
	entry := NewEntry(key, value, PUT)
	// 追加到数据文件当中
	err = db.df.Write(entry)
	// 写到内存
	db.indexes[string(key)] = offset
	return
}

func (db *AliDB) Get(key []byte) (value []byte, err error) {

	if len(key) == 0 {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.indexes[string(key)]

	if !ok {
		return
	}
	// 从磁盘中读取数据
	var e *Entry
	e, err = db.df.Read(val)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		value = e.Value
	}
	return
}

func (db *AliDB) Delete(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// 从内存当中取出索引信息
	_, ok := db.indexes[string(key)]
	// key 不存在，忽略
	if !ok {
		return
	}

	// 封装成 Entry 并写入
	e := NewEntry(key, nil, DEL)
	err = db.df.Write(e)
	if err != nil {
		return
	}

	delete(db.indexes, string(key))
	return
}
