package internal

import (
	"fmt"
	"time"
)

var slowQueryThreshold time.Duration

var slowQueryAlertThreshold time.Duration
var ShowSql bool
var maxOpenConns int
var maxIdleConns int
var connMaxLifetime time.Duration
var connMaxIdletime time.Duration

var readTimeoutS = "5s"
var writeTimeoutS = "5s"
var timeoutS = "1s"

func GetDSN(dbName string) (dsn string, err error) {
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=True&loc=Local&timeout=%s&readTimeout=%s&writeTimeout=%s",
		"Username", "Password", "Host", "Port", "Dbname", "Charset", timeoutS, readTimeoutS, writeTimeoutS)
	return dsn, nil
}
