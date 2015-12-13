package main

import (
	"flag"
)

// no master
// no single point of failure
// shared nothing
// no distributed state
// plugin system
// replication
// reed solomon erasure coding

func init() {
	flag.StringVar(&confSeedFlag, "seeds", "", "Seeds list (abc_host:port,xyz_host:port)")
	flag.Parse()
}

func main() {
	conf = newConf()
	restServer = newRestServer()
	gossip = newGossip()
	<-shutdown
}
