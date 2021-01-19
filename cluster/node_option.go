package cluster

import (
	"fmt"
)

type NodeOptions struct {
	tableSelector TableSelector
	db            *DB
	tableNum      uint64
	dbIndex       int
	identity      string
}

type TableSelector interface {
	Table(originName string, num uint64, index int, values ...interface{}) string
}

type NodeOption func(*NodeOptions)

func WithTableSelector(selecter TableSelector) NodeOption {
	return func(o *NodeOptions) {
		o.tableSelector = selecter
	}
}

func WithDB(db *DB) NodeOption {
	return func(o *NodeOptions) {
		o.db = db
	}
}

func WithDBIndex(index int) NodeOption {
	return func(o *NodeOptions) {
		o.dbIndex = index
	}
}

func WithTableNum(num uint64) NodeOption {
	return func(o *NodeOptions) {
		o.tableNum = num
	}
}

func WithIdentity(identity string) NodeOption {
	return func(o *NodeOptions) {
		o.identity = identity
	}
}

type TableSelectorFunc func(originName string, num uint64, index int, values ...interface{}) string

func (d TableSelectorFunc) Table(originName string, num uint64, index int, values ...interface{}) string {
	return d(originName, num, index, values...)
}

func tableSelector(originName string, num uint64, index int, values ...interface{}) string {
	if num == 1 {
		return originName
	}

	if len(values) != 1 {
		panic("default table num must be 1")
	}

	value, ok := values[0].(int64)
	if !ok {
		panic("value must be int64")
	}

	return fmt.Sprintf("%v_%08d", originName, int64(index)*int64(num)+value%int64(num))
}

type DB struct {
	Driver     string `default:"mysql"`
	DataSource string
	DBName     string
	UserName   string
	Password   string
	Host       string
	Port       int

	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifeTime int64
}
