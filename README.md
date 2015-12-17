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