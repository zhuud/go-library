package internal

import (
	"errors"
	"sync"

	mysqlraw "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TODO 实现 熔断 重试 超时 日志
type DBWrapper struct {
	DBName string
	*gorm.DB
	once sync.Once
}

type mysqlDialector struct {
	*mysql.Dialector
}

func (d *mysqlDialector) Translate(err error) error {
	err = d.Dialector.Translate(err)
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return gorm.ErrRecordNotFound
	}
	logx.ErrorStack(err)
	return err
}

func (w *DBWrapper) Init() {
	w.once.Do(func() {
		dsn, err := GetDSN(w.DBName)
		if err != nil {
			panic(err)
		}
		dsnConf, _ := mysqlraw.ParseDSN(dsn)
		var dialector gorm.Dialector = &mysqlDialector{
			Dialector: &mysql.Dialector{
				Config: &mysql.Config{
					DSN:       dsn,
					DSNConfig: dsnConf,
				},
			},
		}

		db, err := gorm.Open(dialector, &gorm.Config{
			SkipDefaultTransaction: true,
			CreateBatchSize:        1000,
			TranslateError:         true,
		})
		if err != nil {
			panic(err)
		}
		rawDB, err := db.DB()
		if err != nil {
			panic(err)
		}
		rawDB.SetMaxIdleConns(maxIdleConns)
		rawDB.SetMaxOpenConns(maxOpenConns)
		rawDB.SetConnMaxIdleTime(connMaxIdletime)
		rawDB.SetConnMaxLifetime(connMaxLifetime)

		if ShowSql {
			db = db.Debug()
		}
		w.DB = db
	})
}
