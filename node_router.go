package main

import (
	"errors"
	"math/rand"
)

// Router to nodes
type NodeRouter struct {
}

// Pick node
func (this *NodeRouter) PickNode() (string, error) {
	// @todo smarter node selection, e.g. https://labs.spotify.com/2015/12/08/els-part-1/
	nodes := gossip.GetNodeStates()
	nodeCount := len(nodes)
	selected := rand.Intn(nodeCount)
	i := 0
	for _, node := range nodes {
		if i == selected {
			return node.Node, nil
		}
		i++
	}
	return "", errors.New("Unable to pick node from router")
}

// New router
func newNodeRouter() *NodeRouter {
	return &NodeRouter{}
}
