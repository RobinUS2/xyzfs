package main

// HTTP REST server for all client interactions

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

var shutdown chan bool = make(chan bool, 1)

var restServer *RestServer

type RestServer struct {
	PrettyPrint bool
}

// Authenticate
func (this *RestServer) auth(r *http.Request) bool {
	// @todo Implement
	return true
}

// Not authorized
func (this *RestServer) notAuthorized(w http.ResponseWriter) {
	w.WriteHeader(http.StatusUnauthorized)
}

// Start
func (this *RestServer) start() {
	go func() {
		// New router
		router := httprouter.New()

		// Debug handlers
		if conf.HttpDebug {
			log.Warn("HTTP debug endpoints are ON")

			// File locator
			router.GET("/debug/file-locator/shards", GetDebugFileLocatorShards)
			router.GET("/debug/file-locator/shard-locations", GetDebugFileLocatorShardLocations)

			// Gossip
			router.GET("/debug/gossip/nodes", GetDebugGossipNodes)
		}

		// Start server
		log.Infof("Starting REST HTTP server on port TCP/%d", conf.HttpPort)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", conf.HttpPort), router))
	}()
}

func newRestServer() *RestServer {
	o := &RestServer{
		PrettyPrint: true,
	}
	o.start()
	return o
}
