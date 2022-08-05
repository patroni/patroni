#! /bin/sh
# create database and user after bootstrap if they are not present.
# use the same variables like in the normal container:
#  POSTGRES_DB
#  POSTRES_USER
#  POSTGRES_PASSWD

connectstring=$1

createdb="SELECT 'CREATE DATABASE "$POSTGRES_DB"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '"$POSTGRES_DB"')\gexec"
createuser="SELECT 'CREATE USER "$POSTGRES_USER"' where not exists (select from pg_user where usename = '"$POSTGRES_USER"')\gexec"
createpasswd="ALTER USER "$POSTGRES_USER" PASSWORD '"$POSTGRES_PASSWORD"';"
giveowner="ALTER DATABASE "$POSTGRES_DB" OWNER TO "$POSTGRES_USER";"

echo $createdb | psql -d $connectstring
echo $createuser | psql -d $connectstring
echo $createpasswd | psql -d $connectstring
echo $giveowner | psql -d $connectstring

exit 0