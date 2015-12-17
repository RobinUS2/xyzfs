# xyzFS
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

Terminology
=============
- cluster: set of nodes that run xyzFS and behave as one virtual large distributed filesystem
- node: (virtual) machine that runs one instance of the xyzFS binary
- volume: location on disk of a node where data is stored
- block: chunk of data that is stored in at least one volume which is replicated
- shard: part of a block which can be either data or parity for erasure coding
- file: representation of a file like in a typical file system