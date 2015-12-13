package main

import (
	"fmt"
	"log"
	"net/http"
)

var restServer *RestServer

type RestServer struct {
}

func (this *RestServer) start() {
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", conf.HttpPort), nil))
}

func newRestServer() *RestServer {
	o := &RestServer{}
	o.start()
	return o
}
