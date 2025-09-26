package gorm

import (
	"github.com/cornelk/hashmap"
	"github.com/zhuud/go-library/svc/gorm/internal"
	"gorm.io/gorm"
)

var singletonDbMap = hashmap.New[string, *internal.DBWrapper]()

func GetDB(dbName string) *gorm.DB {
	return getDBWrapper(dbName).DB
}

func getDBWrapper(dbName string) *internal.DBWrapper {
	w, _ := singletonDbMap.GetOrInsert(dbName, &internal.DBWrapper{
		DBName: dbName,
	})
	w.Init()
	return w
}
