#!/bin/bash

[[ "$3" == "master" ]] || exit

PGPASSWORD=zalando psql -h localhost -U postgres -p $1 -w -tAc "SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'logical'" >> data/postgres0/label
