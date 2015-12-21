package main

import (
	"io/ioutil"
	"os"
	"strings"
)

var conf *Conf
var confSeedFlag string

type Conf struct {
	HttpPort                  int
	Datastore                 *DatastoreConf
	Seeds                     []string
	DataShardsPerBlock        int
	ParityShardsPerBlock      int
	ShardSizeInBytes          int
	UnixFolderPermissions     os.FileMode
	UnixFilePermissions       os.FileMode
	MetaBasePath              string
	VolumeBasePath            string
	GossipPort                int
	GossipHelloInterval       uint32
	GossipTransportReadBuffer int
	BinaryPort                int
	BinaryUdpPort             int
	BinaryTransportReadBuffer int
}

type DatastoreConf struct {
	Volumes []*Volume
}

func newConf() *Conf {
	c := &Conf{
		// Network
		HttpPort:      8080,
		GossipPort:    3322,
		BinaryPort:    3323,
		BinaryUdpPort: 3324,

		// Shards
		DataShardsPerBlock:   10,
		ParityShardsPerBlock: 3,
		ShardSizeInBytes:     1024 * 1024 * 32,

		// Permission
		UnixFolderPermissions: 0755,
		UnixFilePermissions:   0644,

		// Storage
		MetaBasePath:   "/xyzfs/meta",
		VolumeBasePath: "/xyzfs/data",

		// Gossip
		GossipHelloInterval:       5,
		GossipTransportReadBuffer: 8 * 1024,

		// Binary
		BinaryTransportReadBuffer: 32 * 1024 * 1024,
	}

	if len(confSeedFlag) > 0 {
		c.Seeds = strings.Split(confSeedFlag, ",")
	}

	return c
}

func (this *Conf) prepareStartup() {
	// Meta folder exists?
	if _, err := os.Stat(this.MetaBasePath); os.IsNotExist(err) {
		// Create
		log.Infof("Creating folder %s", this.MetaBasePath)
		e := os.MkdirAll(this.MetaBasePath, conf.UnixFolderPermissions)
		if e != nil {
			log.Errorf("Failed to create %s: %s", this.MetaBasePath, e)
		}
	}

	// Volume folder exists?
	if _, err := os.Stat(this.VolumeBasePath); os.IsNotExist(err) {
		// Create
		log.Infof("Creating folder %s", this.VolumeBasePath)
		e := os.MkdirAll(this.VolumeBasePath, conf.UnixFolderPermissions)
		if e != nil {
			log.Errorf("Failed to create %s: %s", this.VolumeBasePath, e)
		}
	}
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
