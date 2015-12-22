package main

import (
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strings"
)

// List shards for file
func GetDebugFileLocatorShards(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Response object
	jr := jresp.NewJsonResp()

	// Auth
	if !restServer.auth(r) {
		restServer.notAuthorized(w)
		return
	}

	// Get filename
	file := strings.TrimSpace(r.URL.Query().Get("filename"))
	if len(file) < 1 {
		jr.Error("Please provide the 'filename' as query parameter")
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Locate
	res, scanCount, e := datastore.LocateFile(file)
	if e != nil {
		jr.Error(fmt.Sprintf("%s", e))
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Shard IDs
	var shardIds []string = make([]string, 0)
	for _, shardIdx := range res {
		shardIds = append(shardIds, uuidToString(shardIdx.ShardId))
	}

	// Response
	jr.Set("shards", shardIds)
	jr.Set("shards_scanned", scanCount)
	jr.OK()
	fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
}

// Map from shard to node locations
func GetDebugFileLocatorShardLocations(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Response object
	jr := jresp.NewJsonResp()

	// Auth
	if !restServer.auth(r) {
		restServer.notAuthorized(w)
		return
	}

	// Response
	jr.Set("shards", datastore.fileLocator.ShardLocations())
	jr.OK()
	fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
}
