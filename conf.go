package main

import (
	"strings"
)

var conf *Conf
var confSeedFlag string

type Conf struct {
	HttpPort  int
	Datastore *DatastoreConf
	Seeds     []string
}

type DatastoreConf struct {
	Volumes []*DatastoreDirectory
}

type DatastoreDirectory struct {
	Path string
}

func newConf() *Conf {
	c := &Conf{
		HttpPort:  8080,
		Datastore: newDatastoreConf(),
	}

	if len(confSeedFlag) > 0 {
		c.Seeds = strings.Split(confSeedFlag, ",")
	}

	return c
}

func newDatastoreConf() *DatastoreConf {
	d := make([]*DatastoreDirectory, 0)
	d = append(d, &DatastoreDirectory{Path: "/xyzfs/data"})

	return &DatastoreConf{
		Volumes: d,
	}
}
