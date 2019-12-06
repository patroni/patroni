#!/usr/bin/env python
import psycopg2
import sys

if not (len(sys.argv) >= 3 and sys.argv[3] == "master"):
    sys.exit()

query="SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'logical'"

connection = psycopg2.connect(user="postgres",
                               password="zalando",
                               host="127.0.0.1",
                               port=sys.argv[1])
cursor = connection.cursor()
cursor.execute(query)

with open("data/postgres0/label", "w") as label:
    label.write(next(iter(cursor.fetchone()), ""))
