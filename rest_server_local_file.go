package main

import (
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"mime"
	"net/http"
	"strings"
)

// Get local file (will not return if the file is not on this node)
func GetLocalFile(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
	indices, _, e := datastore.LocateFile(file)
	if e != nil {
		jr.Error(fmt.Sprintf("%s", e))
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Find local shard
	var fileReadErr error
	var fileBytes []byte = nil
	for _, resIdx := range indices {
		shard := datastore.LocalShardByIdStr(uuidToString(resIdx.ShardId))
		if shard == nil {
			continue
		}
		fileBytes, fileReadErr, _ = shard.ReadFile(file)
		if fileReadErr == nil {
			break
		}
	}
	// Filename
	fileNameSplit := strings.Split(file, "/")
	fileBaseName := fileNameSplit[len(fileNameSplit)-1]
	fileBaseNameDotSplit := strings.Split(fileBaseName, ".")
	fileExt := fileBaseNameDotSplit[len(fileBaseNameDotSplit)-1]

	// Content type
	fileContentType := mime.TypeByExtension(fmt.Sprintf(".%s", fileExt))

	// Headers
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileBaseName))
	w.Header().Set("Content-Type", fileContentType)
	w.Write(fileBytes)
}
