package main

import (
	"strings"
)

type TransportConnectionMeta struct {
	RemoteAddr string
	node       string
}

func (this *TransportConnectionMeta) GetNode() string {
	return this.node
}

func newTransportConnectionMeta(remoteAddr string) *TransportConnectionMeta {
	o := &TransportConnectionMeta{
		RemoteAddr: remoteAddr,
	}
	remoteAddrSplit := strings.Split(o.RemoteAddr, ":")
	o.node = remoteAddrSplit[0]
	return o
}
