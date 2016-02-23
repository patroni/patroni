# startup scripts for Patroni

This directory contains sample startup scripts for various OSes 
and management tools for Patroni.

Scripts supplied:

### patroni.upstart.conf

Upstart job for Ubuntu 12.04 or 14.04.  Requires Upstart > 1.4. Intended for systems where Patroni has been installed on a base system, rather than in Docker.

### patroni.service
Systemd service file, to be copied to /etc/systemd/system/patroni.service, tested on Centos 7.1 with Patroni installed from pip. 
