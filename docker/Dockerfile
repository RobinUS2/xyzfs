# Source
FROM centos:7

# Extend base tools (do NOT install xyzfs related tools here)
RUN yum -y install sudo

# Networking
EXPOSE 8080
EXPOSE 3322
EXPOSE 3323
EXPOSE 3324

# Make user
RUN useradd xyzfs

# Folders
RUN mkdir -p /xyzfs
RUN mkdir -p /usr/local/xyzfs/bin
RUN chown -R xyzfs:xyzfs /xyzfs
RUN chown -R xyzfs:xyzfs /usr/local/xyzfs

# Copy binary
ADD ./xyzfs /usr/local/xyzfs/bin/xyzfs

# TTY-less sudo
RUN touch /tmp/sudoers
RUN cat /etc/sudoers | grep -v "requiretty" > /tmp/sudoers
RUN mv /tmp/sudoers /etc/sudoers

# Container
CMD sudo -u xyzfs /usr/local/xyzfs/bin/xyzfs
