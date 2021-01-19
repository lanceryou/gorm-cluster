package cluster

import (
	"math/rand"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type ShardingOptions struct {
	balancer Balancer
	master   *ClusterNode
	slaves   []*ClusterNode
}

type Balancer interface {
	Next([]*ClusterNode) *ClusterNode
}

type ShardingOption func(*ShardingOptions)

func WithBalancer(selector Balancer) ShardingOption {
	return func(o *ShardingOptions) {
		o.balancer = selector
	}
}

func WithMaster(m *ClusterNode) ShardingOption {
	return func(o *ShardingOptions) {
		o.master = m
	}
}

func WithSlaves(s []*ClusterNode) ShardingOption {
	return func(o *ShardingOptions) {
		o.slaves = s
	}
}

type BalancerFunc func([]*ClusterNode) *ClusterNode

func (d BalancerFunc) Next(s []*ClusterNode) *ClusterNode {
	return d(s)
}

func RoundRobin() BalancerFunc {
	var mtx sync.Mutex
	var i int
	return func(nodes []*ClusterNode) *ClusterNode {
		mtx.Lock()
		node := nodes[i%len(nodes)]
		i++
		if i >= len(nodes) {
			i = 0
		}
		mtx.Unlock()
		return node
	}
}

func Random() BalancerFunc {
	return func(nodes []*ClusterNode) *ClusterNode {
		return nodes[rand.Int()%len(nodes)]
	}
}
