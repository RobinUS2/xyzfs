package main

import (
	"os"
	"strings"
)

var conf *Conf
var confSeedFlag string

type Conf struct {
	HttpPort              int
	Datastore             *DatastoreConf
	Seeds                 []string
	DataShardsPerBlock    int
	ParityShardsPerBlock  int
	ShardSizeInBytes      int
	UnixFolderPermissions os.FileMode
}

type DatastoreConf struct {
	Volumes []*Volume
}

func newConf() *Conf {
	c := &Conf{
		HttpPort:              8080,
		DataShardsPerBlock:    10,
		ParityShardsPerBlock:  3,
		UnixFolderPermissions: 0655,
		ShardSizeInBytes:      1024 * 1024 * 32,
		Datastore:             newDatastoreConf(),
	}

	if len(confSeedFlag) > 0 {
		c.Seeds = strings.Split(confSeedFlag, ",")
	}

	return c
}

func newDatastoreConf() *DatastoreConf {
	d := make([]*Volume, 0)
	d = append(d, &Volume{
		Path: "/xyzfs/data",
		Id:   randomUuid(),
	})

	return &DatastoreConf{
		Volumes: d,
	}
}
