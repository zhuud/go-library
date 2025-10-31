package gorm

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type ctxKey string

const transactionContextKey ctxKey = "gorm.transaction"

type transactionContext struct {
	ctx  context.Context
	db   *gorm.DB
	done int32
}

type Dao[M any, ID comparable] interface {
	DB(ctx context.Context) *gorm.DB
	Transaction(ctx context.Context, fn func(ctx context.Context, tx *gorm.DB) error) error

	Create(ctx context.Context, data any) error // 支持单条和批量
	UpdateByWhere(ctx context.Context, where string, args []any, data any, columns ...string) error
	DeleteByWhere(ctx context.Context, where string, args []any) error
	GetByPk(ctx context.Context, id ID) (*M, bool, error)
	FindOneByWhere(ctx context.Context, where string, args []any, columns ...string) (*M, bool, error)
	FindListByWhere(ctx context.Context, where string, args []any, columns ...string) ([]*M, error)
}

type BaseDao[M any, ID comparable] struct {
	db          *gorm.DB
	tableName   string
	pkName      string
	tableSchema *schema.Schema
	pkSchema    *schema.Field
}

func NewBaseDao[M any, ID comparable](dbName, tableName, pkName string) *BaseDao[M, ID] {
	db := GetDB(dbName)
	baseDao := &BaseDao[M, ID]{
		db:        db,
		tableName: tableName,
		pkName:    pkName,
	}

	// 解析表结构元数据
	one := new(M)
	tmp := db.Model(one)
	err := tmp.Statement.Parse(one)
	if err != nil {
		// 这里建议用日志而不是 panic，便于排查
		logx.Errorf("gorm.NewBaseDao metadata parsing failed struct: %+v, error: %v", one, err)
	}
	baseDao.tableSchema = tmp.Statement.Schema
	for _, f := range tmp.Statement.Schema.Fields {
		if f.DBName == pkName {
			baseDao.pkSchema = f
		}
	}
	return baseDao
}

func (dao *BaseDao[M, ID]) DB(ctx context.Context) *gorm.DB {
	v := ctx.Value(transactionContextKey)
	if v != nil {
		txCtx := v.(*transactionContext)
		return txCtx.db.WithContext(ctx).Model(new(M)).Table(dao.tableName)
	}
	return dao.db.WithContext(ctx).Model(new(M)).Table(dao.tableName)
}

func (dao *BaseDao[M, ID]) Transaction(ctx context.Context, fn func(ctx context.Context, tx *gorm.DB) error) error {
	v := ctx.Value(transactionContextKey)
	if v != nil {
		tc, ok := v.(*transactionContext)
		if ok && tc != nil && atomic.LoadInt32(&tc.done) == 0 {
			return fn(tc.ctx, tc.db)
		}
	}
	txCtx := &transactionContext{ctx: ctx}
	err := dao.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txCtx.db = tx
		newCtx := context.WithValue(ctx, transactionContextKey, txCtx)
		return fn(newCtx, tx)
	})
	if err == nil {
		atomic.StoreInt32(&txCtx.done, 1)
	}
	return err
}

func (dao *BaseDao[M, ID]) Create(ctx context.Context, m *M) error {
	return dao.DB(ctx).Create(m).Error
}

func (dao *BaseDao[M, ID]) BatchCreate(ctx context.Context, ms []*M) error {
	if len(ms) == 0 {
		return nil
	}
	return dao.DB(ctx).Create(ms).Error
}

func (dao *BaseDao[M, ID]) UpdateByWhere(ctx context.Context, where string, args []any, data any, columns ...string) (int64, error) {
	tx := dao.DB(ctx).Where(where, args...)
	if len(columns) > 0 {
		tx.Select(columns)
	}
	ret := tx.Updates(data)
	return ret.RowsAffected, ret.Error
}

func (dao *BaseDao[M, ID]) DeleteByWhere(ctx context.Context, where string, args []any) (int64, error) {
	ret := dao.DB(ctx).Where(where, args...).Delete(new(M))
	return ret.RowsAffected, ret.Error
}

func (dao *BaseDao[M, ID]) GetByPk(ctx context.Context, id ID) (*M, bool, error) {
	var m M
	err := dao.DB(ctx).First(&m, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	return &m, err == nil, err
}

func (dao *BaseDao[M, ID]) FindOneByWhere(ctx context.Context, where string, args []any, columns ...string) (*M, bool, error) {
	var m M
	tx := dao.DB(ctx).Where(where, args...)
	if len(columns) != 0 {
		tx = tx.Select(strings.Join(columns, ", "))
	}
	err := tx.First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	return &m, err == nil, err
}

func (dao *BaseDao[M, ID]) FindListByWhere(ctx context.Context, query string, args []any, columns ...string) ([]*M, error) {
	var list []*M
	tx := dao.DB(ctx).Where(query, args...)
	if len(columns) != 0 {
		tx = tx.Select(strings.Join(columns, ", "))
	}
	tx = tx.Order(dao.pkName + " DESC")
	tx.Find(&list)
	return list, tx.Error
}
