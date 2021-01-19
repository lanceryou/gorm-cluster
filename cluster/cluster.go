package cluster

import (
	"fmt"
)

type Cluster struct {
	opt          Options
	shardingList []*Sharding
}

func (c *Cluster) DBNum() int {
	return c.opt.dbNum
}

func (c *Cluster) TableNum() int {
	return c.opt.tableNum
}

// values sharding 的值
func (c *Cluster) DB(values ...interface{}) *Sharding {
	// 根据value get shard 计算库
	idx := c.opt.selector.Number(c.opt.dbNum, values...)
	if int(idx) >= len(c.shardingList) {
		panic(fmt.Sprintf("selecter db num more than max number:%v max:%v", idx, len(c.shardingList)))
	}

	sh := c.shardingList[idx].clone()
	sh.ShardingValues = values
	return sh
}

func (c *Cluster) Sharding(fn func(sharding *Sharding)) {
	for _, s := range c.shardingList {
		fn(s)
	}
}

func (c *Cluster) SetDBSelector(selector DBSelector) {
	c.opt.selector = selector
}

func NewCluster(opts ...Option) *Cluster {
	var opt Options
	for _, o := range opts {
		o(&opt)
	}

	if opt.dbNum == 0 {
		opt.dbNum = 1
	}

	if opt.selector == nil {
		opt.selector = DBSelectorFunc(dbSelector)
	}

	return &Cluster{
		opt:          opt,
		shardingList: opt.sharding,
	}
}

func NewClusterWithConfig(config *GormClusterConfig) *Cluster {
	var shardings []*Sharding
	if config.DBNum == 0 {
		config.DBNum = 1
	}

	if config.TableNum == 0 {
		config.TableNum = 1
	}

	for i := 0; i < config.DBNum && len(config.Sharding) == 0; i++ {
		shardings = append(shardings, config.ShardingDB(i))
	}

	for _, master := range config.Sharding {
		master.TableNum = config.TableNum
		master.DBNum = config.DBNum
		fmt.Printf("db %v %v\n", master.DataSource, master.DBIndex)

		shardings = append(shardings, master.ShardingDB(master.DBIndex))
	}

	return NewCluster(
		WithDBNum(config.DBNum),
		WithTables(int(config.TableNum)),
		WithShardings(shardings...),
	)
}
