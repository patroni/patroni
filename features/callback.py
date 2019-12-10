#!/usr/bin/env python
import os
import psycopg2
import sys


if __name__ == '__main__':
    if not (len(sys.argv) >= 3 and sys.argv[3] == "master"):
        sys.exit(1)

    os.environ['PGPASSWORD'] = 'zalando'
    connection = psycopg2.connect(host='127.0.0.1', port=sys.argv[1], user='postgres')
    cursor = connection.cursor()
    cursor.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'logical'")

    with open("data/postgres0/label", "w") as label:
        label.write(next(iter(cursor.fetchone()), ""))
