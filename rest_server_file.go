package main

import (
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
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

	// @todo read real body data and validate

	// Add file
	res, resE := datastore.AddFile(file, []byte("Test data"))
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
