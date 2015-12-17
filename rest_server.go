package main

// HTTP REST server for all client interactions

import (
	"fmt"
	"net/http"
)

var shutdown chan bool = make(chan bool, 1)

var restServer *RestServer

type RestServer struct {
}

func (this *RestServer) start() {
	go func() {
		log.Infof("Starting REST HTTP server on port TCP/%d", conf.HttpPort)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", conf.HttpPort), nil))
	}()
}

func newRestServer() *RestServer {
	o := &RestServer{}
	o.start()
	return o
}
