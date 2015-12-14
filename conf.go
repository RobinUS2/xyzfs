package main

import (
	"io/ioutil"
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
	UnixFilePermissions   os.FileMode
	VolumeBasePath        string
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
		UnixFilePermissions:   0655,
		ShardSizeInBytes:      1024 * 1024 * 32,
		VolumeBasePath:        "/xyzfs/data",
	}

	if len(confSeedFlag) > 0 {
		c.Seeds = strings.Split(confSeedFlag, ",")
	}

	return c
}

func newDatastoreConf() *DatastoreConf {
	d := make([]*Volume, 0)

	// Read volumes on disk
	vcs := recoverVolumeConfiguration()
	if vcs != nil {
		for _, vc := range vcs {
			d = append(d, vc)
		}
	}

	// Default volume?
	if len(d) == 0 {
		v := newVolume()
		v.Id = randomUuid()
		v.Path = conf.VolumeBasePath
		d = append(d, v)
	}

	return &DatastoreConf{
		Volumes: d,
	}
}

// Recover volume configuration from local disk
func recoverVolumeConfiguration() []*Volume {
	list, e := ioutil.ReadDir(conf.VolumeBasePath)
	if e != nil {
		log.Errorf("Failed to list volumes in %s: %s", conf.VolumeBasePath, e)
		return nil
	}

	// Results
	d := make([]*Volume, 0)

	// Iterate
	for _, elm := range list {
		split := strings.Split(elm.Name(), "=")
		// Must be in format v=UUID
		if len(split) != 2 || split[0] != "v" {
			continue
		}

		// Init
		v := newVolume()
		v.Id = uuidStringToBytes(split[1])
		v.Path = conf.VolumeBasePath
		d = append(d, v)
	}
	return d
}
