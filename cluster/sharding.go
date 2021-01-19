package cluster

import (
	"database/sql"
)

type Sharding struct {
	master *ClusterNode
	slaves []*ClusterNode

	opt ShardingOptions

	ShardingValues []interface{}
}

func NewSharding(opts ...ShardingOption) *Sharding {
	var opt ShardingOptions
	for _, o := range opts {
		o(&opt)
	}

	if opt.balancer == nil {
		opt.balancer = RoundRobin()
	}

	if opt.master == nil {
		panic("must has master")
	}

	if len(opt.slaves) == 0 {
		opt.slaves = append(opt.slaves, opt.master)
	}

	return &Sharding{
		master: opt.master,
		slaves: opt.slaves,
		opt:    opt,
	}
}

func (n *Sharding) clone() *Sharding {
	return &Sharding{
		master: n.master,
		slaves: n.slaves,
		opt:    n.opt,
	}
}

func (n *Sharding) SetBalancer(balancer Balancer) {
	n.opt.balancer = balancer
}

func (n *Sharding) ClusterNode(fn func(node *ClusterNode)) {
	fn(n.master)
	for _, s := range n.slaves {
		fn(s)
	}
}

func (n *Sharding) Open() error {
	if err := n.master.Open(); err != nil {
		return err
	}

	for _, slave := range n.slaves {
		if err := slave.Open(); err != nil {
			return err
		}
	}
	return nil
}

// Save update value in database, if the value doesn't have primary key, will insert it
func (n *Sharding) Save(value interface{}) *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Save(value)
}

// Create insert the value into database
func (n *Sharding) Create(value interface{}) *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Create(value)
}

// Delete delete value match given conditions, if the value has primary key, then will including the primary key as condition
func (n *Sharding) Delete(value interface{}) *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Delete(value)
}

// Scan scan value to a struct
func (n *Sharding) Scan(dest interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Scan(dest)
}

// Row return `*sql.Row` with given conditions
func (n *Sharding) Row() *sql.Row {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Row()
}

// Rows return `*sql.Rows` with given conditions
func (n *Sharding) Rows() (*sql.Rows, error) {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Rows()
}

// ScanRows scan `*sql.Rows` to give struct
func (n *Sharding) ScanRows(rows *sql.Rows, result interface{}) error {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).ScanRows(rows, result)
}

func (n *Sharding) Raw(sql string, values ...interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Raw(sql, values)
}

func (n *Sharding) Exec(sql string, values ...interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Exec(sql, values)
}

// FirstOrCreate find first matched record or create a new one with given conditions (only works with struct, map conditions)
// https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (n *Sharding) FirstOrCreate(out interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).FirstOrCreate(out)
}

// First find first record that match given conditions, order by primary key
func (n *Sharding) First(out interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).First(out)
}

func (n *Sharding) Last(out interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Last(out)
}

// Updates update attributes with callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (n *Sharding) Updates(values interface{}) *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Updates(values)
}

// UpdateColumns update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (n *Sharding) UpdateColumns(values interface{}) *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).UpdateColumns(values)
}

// Begin begin a transaction
func (n *Sharding) Begin() *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Begin()
}

// Commit commit a transaction
func (n *Sharding) Commit() *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Commit()
}

// Rollback rollback a transaction
func (n *Sharding) Rollback() *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Rollback()
}

// Find find records that match given conditions
func (n *Sharding) Find(out interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Find(out)
}

// Where return a new relation, filter records with given conditions, accepts `map`, `struct` or `string` as conditions, refer http://jinzhu.github.io/gorm/crud.html#query
func (n *Sharding) Where(query interface{}, args ...interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Where(query, args...)
}

// Or filter records that match before conditions or this one, similar to `Where`
func (n *Sharding) Or(query interface{}, args ...interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Or(query, args...)
}

// Not filter records that don't match current conditions, similar to `Where`
func (n *Sharding) Not(query interface{}, args ...interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Not(query, args...)
}

// Limit specify the number of records to be retrieved
func (n *Sharding) Limit(limit interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Limit(limit)
}

// Offset specify the number of records to skip before starting to return the records
func (n *Sharding) Offset(offset interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Offset(offset)
}

// Model specify the model you would like to run db operations
//    // update all users's name to `hello`
//    db.Model(&User{}).Update("name", "hello")
//    // if user's primary key is non-blank, will use it as condition, then will only update the user's name to `hello`
//    db.Model(&user).Update("name", "hello")
func (n *Sharding) Model(value interface{}) *ClusterNode {
	return (&ClusterNode{db: n.master.db, opts: n.master.opts, ShardingValues: n.ShardingValues}).Model(value)
}

// Order specify order when retrieve records from database, set reorder to `true` to overwrite defined conditions
//     db.Order("name DESC")
//     db.Order("name DESC", true) // reorder
//     db.Order(gorm.Expr("name = ? DESC", "first")) // sql expression
func (n *Sharding) Order(value interface{}, reorder ...bool) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Order(value, reorder...)
}

// Select specify fields that you want to retrieve from database when querying, by default, will select all fields;
// When creating/updating, specify fields that you want to save to database
func (n *Sharding) Select(query interface{}, args ...interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Select(query, args...)
}

// Count get how many records for a model
func (n *Sharding) Count(value interface{}) *ClusterNode {
	node := n.opt.balancer.Next(n.slaves)
	return (&ClusterNode{db: node.db, opts: node.opts, ShardingValues: n.ShardingValues}).Count(value)
}
