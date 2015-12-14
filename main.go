package main

import (
	"flag"
	"sync"
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
	startApplication()

	// Wait for shutdown
	<-shutdown
}

var startApplicationOnce sync.Once

func startApplication() {
	startApplicationOnce.Do(func() {
		log.Info("Starting xyzFS")

		// Basic config
		conf = newConf()

		// Data store config
		conf.Datastore = newDatastoreConf()

		// Datatastore
		datastore = newDatastore()

		// HTTP server
		restServer = newRestServer()

		// Gossip with other nodes
		gossip = newGossip()
	})
}
