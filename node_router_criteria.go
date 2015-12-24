package main

// Set of criteria which node router should (try to) take into account

type NodeRouterCriteria struct {
	ExcludeLocalNodes bool
}

func newNodeRouterCriteria() *NodeRouterCriteria {
	return &NodeRouterCriteria{}
}
