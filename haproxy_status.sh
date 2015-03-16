#!/bin/bash

while true
do {
        response=$(echo "SELECT pg_is_in_recovery();" | psql -t postgres --port $2 --host $1 2> /dev/null)

        if [ "${response}" == " f" ]
        then
                echo "HTTP/1.1 200 OK"
                echo "X-XLOG-POSITION: $(echo "SELECT pg_current_xlog_location();" | psql -t postgres 2> /dev/null | tr -d ' ' | head -n 1)"
        else
                echo "HTTP/1.1 503 Service unavailable"
                echo "X-XLOG-POSITION: $(echo "SELECT pg_last_xlog_replay_location();" | psql -t postgres 2> /dev/null | tr -d ' ' | head -n 1)"
        fi

} | nc -l $3; done
