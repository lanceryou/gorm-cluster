package cluster

import "fmt"

type GormClusterConfig struct {
	DBNum      int
	TableNum   uint64
	DBIndex    int
	DataSource string
	DBName     string
	UserName   string
	Password   string
	Host       string
	Port       int

	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifeTime int64
	Slaves          []*DB
	Sharding        []*GormClusterConfig
}

func (o *GormClusterConfig) Master(dbIdx int) *DB {
	dbName := o.DBName
	if o.DBNum > 1 {
		dbName = fmt.Sprintf("%v_%08d", dbName, dbIdx)
	}
	return &DB{
		DBName:          dbName,
		DataSource:      o.DataSource,
		UserName:        o.UserName,
		Password:        o.Password,
		Host:            o.Host,
		Port:            o.Port,
		MaxOpenConns:    o.MaxOpenConns,
		MaxIdleConns:    o.MaxIdleConns,
		ConnMaxLifeTime: o.ConnMaxLifeTime,
	}
}

func (o *GormClusterConfig) SlavesDB(dbIdx int) (ss []*DB) {
	for _, slave := range o.Slaves {
		dbName := slave.DBName
		if o.DBNum > 1 {
			dbName = fmt.Sprintf("%v_%08d", dbName, dbIdx)
		}
		ss = append(ss, &DB{
			DBName:          dbName,
			DataSource:      slave.DataSource,
			UserName:        slave.UserName,
			Password:        slave.Password,
			Host:            slave.Host,
			Port:            slave.Port,
			MaxOpenConns:    slave.MaxOpenConns,
			MaxIdleConns:    slave.MaxIdleConns,
			ConnMaxLifeTime: slave.ConnMaxLifeTime,
		})
	}
	return
}

func (o *GormClusterConfig) ShardingDB(dbIdx int) *Sharding {
	var slaves []*ClusterNode
	for _, s := range o.SlavesDB(dbIdx) {
		slaves = append(slaves, NewClusterNode(
			WithTableNum(o.TableNum),
			WithDB(s),
			WithDBIndex(dbIdx),
			WithIdentity("slave")),
		)
	}

	master := NewClusterNode(
		WithTableNum(o.TableNum),
		WithDB(o.Master(dbIdx)),
		WithDBIndex(dbIdx),
		WithIdentity("master"))

	return NewSharding(
		WithMaster(master),
		WithSlaves(slaves),
	)
}
