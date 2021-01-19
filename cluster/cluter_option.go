package cluster

type Options struct {
	dbNum    int
	tableNum int
	selector DBSelector
	sharding []*Sharding
}

type DBSelector interface {
	Number(num int, values ...interface{}) uint64
}

type Option func(*Options)

func WithDBNum(num int) Option {
	return func(o *Options) {
		o.dbNum = num
	}
}

func WithTables(num int) Option {
	return func(o *Options) {
		o.tableNum = num
	}
}

func WithDBSelector(selector DBSelector) Option {
	return func(o *Options) {
		o.selector = selector
	}
}

func WithShardings(sharding ...*Sharding) Option {
	return func(o *Options) {
		o.sharding = sharding
	}
}

type DBSelectorFunc func(num int, values ...interface{}) uint64

func (d DBSelectorFunc) Number(num int, values ...interface{}) uint64 {
	return d(num, values...)
}

func dbSelector(num int, values ...interface{}) uint64 {
	if num == 1 {
		return 0
	}

	if len(values) != 1 {
		panic("default sharding db values len must be 1")
	}

	value, ok := values[0].(int64)
	if !ok {
		panic("value must be int64")
	}

	return uint64(uint64(value) % uint64(num))
}
