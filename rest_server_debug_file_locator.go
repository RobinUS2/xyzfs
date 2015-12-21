package main

import (
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strings"
)

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
	res, e := datastore.LocateFile(file)
	if e != nil {
		jr.Error(fmt.Sprintf("%s", e))
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	var shardIds []string = make([]string, 0)
	for _, shardIdx := range res {
		shardIds = append(shardIds, uuidToString(shardIdx.ShardId))
	}

	// Response
	jr.Set("shards", shardIds)
	jr.OK()
	fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
}
