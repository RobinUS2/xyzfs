package main

import (
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"strings"
)

// Create new file
func PostFile(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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

	// Read body (supports GZIP)
	b, be := readBodyBytes(r)
	if be != nil {
		jr.Error(fmt.Sprintf("%s", be))
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Maximum file size
	if len(b) > conf.MaxFileSize {
		jr.Error("File exceeds maximum file size")
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// @todo is this a new file? In that case we have to modify it

	// Add file
	res, resE := datastore.AddFile(file, b)
	if resE != nil {
		jr.Error(fmt.Sprintf("%s", resE))
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Response
	jr.Set("created", res)
	jr.OK()
	fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
}

// Get file
func GetFile(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
	res, _, e := datastore.LocateFile(file)
	if e != nil {
		jr.Error(fmt.Sprintf("%s", e))
		fmt.Fprint(w, jr.ToString(restServer.PrettyPrint))
		return
	}

	// Shard IDs
outer:
	for _, shardIdx := range res {
		locations := datastore.fileLocator.ShardLocationsByIdStr(uuidToString(shardIdx.ShardId))
		for _, location := range locations {
			log.Infof("%v", location)

			// Request
			uri := fmt.Sprintf("http://%s:%d/v1/local/file?filename=%s", location.Node, conf.HttpPort, file)
			resp, err := http.Get(uri)
			if err != nil {
				log.Warnf("Failed to request %s: %s", uri, err)

				// Attempt nex tlocation
				continue
			}
			// Read body
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)

			// @todo forward headers

			// Output body
			w.Write(body)

			// Done
			break outer
		}
	}
}
