package cluster

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-gorm/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type ClusterNode struct {
	db   *gorm.DB
	opts NodeOptions

	ShardingValues []interface{}
}

func NewClusterNode(opts ...NodeOption) *ClusterNode {
	opt := NodeOptions{
		tableNum:      1,
		tableSelector: TableSelectorFunc(tableSelector),
	}

	for _, o := range opts {
		o(&opt)
	}

	if opt.db == nil {
		panic("db option nil")
	}

	if opt.db.MaxIdleConns == 0 {
		opt.db.MaxIdleConns = 200
	}

	if opt.db.MaxOpenConns == 0 {
		opt.db.MaxOpenConns = 200
	}

	if opt.db.ConnMaxLifeTime == 0 {
		opt.db.ConnMaxLifeTime = 60
	}

	if opt.db.Port == 0 {
		opt.db.Port = 3306
	}

	return &ClusterNode{
		opts: opt,
	}
}

func (n *ClusterNode) SetTableSelector(selector TableSelector) {
	n.opts.tableSelector = selector
}

func (n *ClusterNode) TableNum() int {
	return int(n.opts.tableNum)
}

func (n *ClusterNode) Master() bool {
	return n.opts.identity == "master"
}

func (n *ClusterNode) Open() error {
	if n.opts.db.DataSource == "" {
		n.opts.db.DataSource = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true&loc=Local",
			n.opts.db.UserName,
			n.opts.db.Password,
			fmt.Sprintf("%v:%v", n.opts.db.Host, n.opts.db.Port),
			n.opts.db.DBName,
		)
	}

	if n.opts.db.Driver == "" {
		n.opts.db.Driver = "mysql"
	}

	db, err := gorm.Open(n.opts.db.Driver, n.opts.db.DataSource)
	if err != nil {
		return err
	}

	db.DB().SetMaxIdleConns(n.opts.db.MaxIdleConns)
	db.DB().SetMaxOpenConns(n.opts.db.MaxOpenConns)
	db.DB().SetConnMaxLifetime(time.Duration(n.opts.db.ConnMaxLifeTime) * time.Second)
	n.db = db
	return nil
}

// Save update value in database, if the value doesn't have primary key, will insert it
func (n *ClusterNode) Save(value interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Save(value)}
}

// Create insert the value into database
func (n *ClusterNode) Create(value interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Create(value)}
}

// Delete delete value match given conditions, if the value has primary key, then will including the primary key as condition
func (n *ClusterNode) Delete(value interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Delete(value)}
}

// Scan scan value to a struct
func (n *ClusterNode) Scan(dest interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Scan(dest)}
}

// Row return `*sql.Row` with given conditions
func (n *ClusterNode) Row() *sql.Row {
	return n.db.Row()
}

// Rows return `*sql.Rows` with given conditions
func (n *ClusterNode) Rows() (*sql.Rows, error) {
	return n.db.Rows()
}

// ScanRows scan `*sql.Rows` to give struct
func (n *ClusterNode) ScanRows(rows *sql.Rows, result interface{}) error {
	return n.db.ScanRows(rows, result)
}

// FirstOrCreate find first matched record or create a new one with given conditions (only works with struct, map conditions)
// https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (n *ClusterNode) FirstOrCreate(out interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.FirstOrCreate(out)}
}

// First find first record that match given conditions, order by primary key
func (n *ClusterNode) First(out interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.First(out)}
}

func (n *ClusterNode) Last(out interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Last(out)}
}

// Updates update attributes with callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (n *ClusterNode) Updates(values interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Updates(values)}
}

// UpdateColumns update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (n *ClusterNode) UpdateColumns(values interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.UpdateColumns(values)}
}

// Begin begin a transaction
func (n *ClusterNode) Begin() *ClusterNode {
	return &ClusterNode{db: n.db.Begin()}
}

// Commit commit a transaction
func (n *ClusterNode) Commit() *ClusterNode {
	return &ClusterNode{db: n.db.Commit()}
}

// Rollback rollback a transaction
func (n *ClusterNode) Rollback() *ClusterNode {
	return &ClusterNode{db: n.db.Rollback()}
}

// Find find records that match given conditions
func (n *ClusterNode) Find(out interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Find(out)}
}

func (n *ClusterNode) Raw(sql string, values ...interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Raw(sql, values...)}
}

func (n *ClusterNode) Exec(sql string, values ...interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Exec(sql, values...)}
}

func (n *ClusterNode) Error() error {
	return n.db.Error
}

// Where return a new relation, filter records with given conditions, accepts `map`, `struct` or `string` as conditions, refer http://jinzhu.github.io/gorm/crud.html#query
func (n *ClusterNode) Where(query interface{}, args ...interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Where(query, args...)}
}

// Or filter records that match before conditions or this one, similar to `Where`
func (n *ClusterNode) Or(query interface{}, args ...interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Or(query, args...)}
}

// Not filter records that don't match current conditions, similar to `Where`
func (n *ClusterNode) Not(query interface{}, args ...interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Not(query, args...)}
}

// Limit specify the number of records to be retrieved
func (n *ClusterNode) Limit(limit interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Limit(limit)}
}

// Offset specify the number of records to skip before starting to return the records
func (n *ClusterNode) Offset(offset interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Offset(offset)}
}

// Model specify the model you would like to run db operations
//    // update all users's name to `hello`
//    db.Model(&User{}).Update("name", "hello")
//    // if user's primary key is non-blank, will use it as condition, then will only update the user's name to `hello`
//    db.Model(&user).Update("name", "hello")
func (n *ClusterNode) Model(value interface{}) *ClusterNode {
	sv := ShardingValue{value: value, shradingValues: n.ShardingValues,
		tableNum: n.opts.tableNum, dbIndex: n.opts.dbIndex, tableSelector: n.opts.tableSelector}
	return &ClusterNode{db: n.db.Table(sv.TableName()).Model(value)}
}

// Order specify order when retrieve records from database, set reorder to `true` to overwrite defined conditions
//     db.Order("name DESC")
//     db.Order("name DESC", true) // reorder
//     db.Order(gorm.Expr("name = ? DESC", "first")) // sql expression
func (n *ClusterNode) Order(value interface{}, reorder ...bool) *ClusterNode {
	return &ClusterNode{db: n.db.Order(value, reorder...)}
}

// Select specify fields that you want to retrieve from database when querying, by default, will select all fields;
// When creating/updating, specify fields that you want to save to database
func (n *ClusterNode) Select(query interface{}, args ...interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Select(query, args...)}
}

// Count get how many records for a model
func (n *ClusterNode) Count(value interface{}) *ClusterNode {
	return &ClusterNode{db: n.db.Count(value)}
}

func (n *ClusterNode) Table(values ...interface{}) *ClusterNode {
	return &ClusterNode{db: n.db, opts: n.opts, ShardingValues: values}
}
