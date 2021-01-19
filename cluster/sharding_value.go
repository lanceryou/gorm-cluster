package cluster

type ShardingValue struct {
	value         interface{}
	tableSelector TableSelector
	tableNum      uint64
	dbIndex       int

	shradingValues []interface{}
}

func (s ShardingValue) TableName() (name string) {
	tn, ok := s.value.(TableName)
	if !ok {
		panic("has not TableName method。")
	}

	name = tn.TableName()
	// 获取 sharding 数据
	return s.tableSelector.Table(name, s.tableNum, s.dbIndex, s.shradingValues...)
}

type TableName interface {
	TableName() string
}
