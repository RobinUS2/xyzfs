package main

import (
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

// List all nodes gossip status
func GetDebugGossipNodes(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Response object
	jr := jresp.NewJsonResp()

	// Auth
	if !restServer.auth(r) {
		restServer.notAuthorized(w)
		return
	}

	// Format
	var list []*GossipNodeState = make([]*GossipNodeState, 0)
	for _, ns := range gossip.GetNodeStates() {
		list = append(list, ns)
	}

	// Response
	jr.Set("nodes", list)
	jr.OK()
	fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
}
