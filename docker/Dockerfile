# Source
FROM centos:7

# Networking
EXPOSE 8080
EXPOSE 3322
EXPOSE 3323
EXPOSE 3324

# Copy binary
ADD ./xyzfs /usr/local/xyzfs/bin/xyzfs

# Container
CMD /usr/local/xyzfs/bin/xyzfs
