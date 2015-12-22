package main

import (
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strings"
)

// Allocate new block
func PostDebugBlockAllocate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Response object
	jr := jresp.NewJsonResp()

	// Auth
	if !restServer.auth(r) {
		restServer.notAuthorized(w)
		return
	}

	// New block
	block := datastore.NewBlock()

	// Response
	jr.Set("block_id", block.IdStr())
	jr.OK()
	fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
}

// Persist existing  block
func PutDebugBlockPersist(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Response object
	jr := jresp.NewJsonResp()

	// Auth
	if !restServer.auth(r) {
		restServer.notAuthorized(w)
		return
	}

	// Id
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if len(id) < 1 {
		jr.Error("Please provide the block 'id' as query parameter")
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Find block
	block := datastore.BlockByIdStr(id)
	if block == nil {
		jr.Error("Block not found")
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Persist
	res := block.Persist()

	// Response
	jr.Set("persisted", res)
	jr.OK()
	fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
}
