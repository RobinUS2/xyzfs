# xyzFS ![Build status](https://api.travis-ci.org/RobinUS2/xyzfs.svg)
Distributed file system. Meant for educational & training purposes.

Building
=============
Download the source
```
git clone git@github.com:RobinUS2/xyzfs.git
cd xyzfs
```

Build in the correct folder
```
./build.sh
```

In order to run the rests (with race build on)
```
./test.sh
```

Docker
=============
In order to run xyzFS in Docker, use the following
```
cd docker
./build_container.sh
./run_container.sh
```

Design principles
=============
- no master
- no single point of failure (SPOF)
- shared nothing
- built-in replication
- reed solomon error correction (erasure coding)
- able to deal with lots of small files
- able to deal with temporary / short-lived files

Terminology
=============
- cluster: set of nodes that run xyzFS and behave as one virtual large distributed filesystem
- node: (virtual) machine that runs one instance of the xyzFS binary
- volume: location on disk of a node where data is stored
- block: chunk of data that is stored in at least one volume which is replicated
- shard: part of a block which can be either data or parity for erasure coding
- file: representation of a file like in a typical file system

Networking
=============
- HTTP 8080: REST API for CRUD operations on the file system
- TCP 3322: Binary gossip between nodes
- TCP 3323: Binary transport between nodes (reliable)
- UDP 3324: Binary transport between nodes

Rough work outlines
=============
- filename ring translation (murmur3)
- implement replication (index over tcp to primary replicas, index over udp to all nodes, contents over tcp to replicaes)
- implement REST POST
- implement REST PUT
- read file from disk (open shard, seek to position, etcd)
- implement REST GET
- implement thombstones to support deletes
- implement REST DELETE
- implement remote shards (indices, no metadata, no contents)
- recover lost shards from parity paritions
- compression (disk, transport, in-memory)
- temporary shards (not persisted to disk, very fast writes/reads)
- writable shards (1-n), where new data is written to
- implement fastest node detection (Expected Latency Selector (ELS) of Spotify)

Design ideas
=============
- versioning of ring (file name hash => node ) translation layer information, that supports adding nodes without increasing latency
