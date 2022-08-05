#! /bin/sh
# create database and user after bootstrap if they are not present.
# use the same variables like in the normal container:
#  POSTGRES_DB
#  POSTRES_USER
#  POSTGRES_PASSWD

connectstring=$*


echo Connecting: $connectstring

createdb="SELECT 'CREATE DATABASE "$POSTGRES_DB"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '"$POSTGRES_DB"')\gexec"
createuser="SELECT 'CREATE USER "$POSTGRES_USER"' where not exists (select from pg_user where usename = '"$POSTGRES_USER"')\gexec"
createpasswd="ALTER USER "$POSTGRES_USER" PASSWORD '"$POSTGRES_PASSWORD"';"
giveowner="ALTER DATABASE "$POSTGRES_DB" OWNER TO "$POSTGRES_USER";"

echo $createdb | psql $connectstring
echo $createuser | psql $connectstring
echo $createpasswd | psql $connectstring
echo $giveowner | psql $connectstring

exit 0