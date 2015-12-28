package main

import (
	"errors"
	"math/rand"
	"time"
)

// Router to nodes
type NodeRouter struct {
}

// Pick node
func (this *NodeRouter) PickNode(criteria *NodeRouterCriteria) (string, error) {
	nodesMap := gossip.GetNodeStates()
	inputNodes := make([]*GossipNodeState, 0)
	for _, ns := range nodesMap {
		inputNodes = append(inputNodes, ns)
	}
	nodeCount := len(inputNodes)

	// No nodes? Add route to localhost
	if nodeCount == 0 {
		inputNodes = append(inputNodes, gossip.GetNodeState(runtime.GetNode()))
	}

	// Apply criteria
	tmp := make([]*GossipNodeState, 0)
	for _, inputNode := range inputNodes {

		// Recent gossipped only
		ts := unixTsUint32()
		minTs := ts - 10
		if inputNode.GetLastHelloReceived() < minTs || inputNode.GetLastHelloSent() < minTs {
			log.Warnf("Ignoring node %s for last gossip (now %d, min ts %d, received %d, sent %d)", inputNode.Node, ts, minTs, inputNode.GetLastHelloReceived(), inputNode.GetLastHelloSent())
			continue
		}

		// Local route?
		if criteria != nil && criteria.ExcludeLocalNodes {
			if inputNode.Node == runtime.GetNode() || inputNode.Node == "localhost" || inputNode.Node == "127.0.0.1" {
				continue
			}
		}
		tmp = append(tmp, inputNode)
	}
	inputNodes = tmp

	// Anything left after filtering?
	if len(inputNodes) < 1 {
		return "", errors.New("Unable to pick node from router")
	}

	// Randomize nodes
	nodes := shuffleGossipNodeStates(inputNodes)

	// Return first node (after shuffling above)
	// @todo smarter node selection, e.g. https://labs.spotify.com/2015/12/08/els-part-1/
	return nodes[0].Node, nil
}

// New router
func newNodeRouter() *NodeRouter {
	return &NodeRouter{}
}

// Randomize order
func shuffleGossipNodeStates(arr []*GossipNodeState) []*GossipNodeState {
	t := time.Now()
	rand.Seed(int64(t.Nanosecond())) // no shuffling without this line

	for i := len(arr) - 1; i > 0; i-- {
		j := rand.Intn(i)
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr
}
