# startup scripts for Patroni

This directory contains sample startup scripts for various OSes
and management tools for Patroni.

Scripts supplied:

### patroni.upstart.conf

Upstart job for Ubuntu 12.04 or 14.04.  Requires Upstart > 1.4. Intended for systems where Patroni has been installed on a base system, rather than in Docker.

### patroni.service
Systemd service file, to be copied to /etc/systemd/system/patroni.service, tested on Centos 7.1 with Patroni installed from pip.

### patroni
Init.d service file for Debian-like distributions. Copy it to /etc/init.d/, make executable:
```chmod 755 /etc/init.d/patroni``` and run with ```service patroni start```, or make it starting on boot with ```update-rc.d patroni defaults```. Also you might edit some configuration variables in it:
PATRONI for patroni.py location
CONF for configuration file
LOGFILE for log (script creates it if does not exist)

Note. If you have several versions of Postgres installed, please add to POSTGRES_VERSION the release number which you wish to run. Script uses this value to append PATH environment with correct path to Postgres bin.
