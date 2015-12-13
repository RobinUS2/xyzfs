package main

// no master
// no single point of failure
// shared nothing
// no distributed state
// plugin system
// replication
// reed solomon erasure coding

func main() {
	conf = newConf()
	restServer = newRestServer()
}
