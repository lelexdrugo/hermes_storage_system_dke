#!/bin/bash

m1=mongo1
port=27017
echo "Starting replica set initialize"
until mongosh --host mongo1:27017 --eval "print(\"waited for connection\")"
do
    sleep 2
done
echo "Connection finished"
echo "Creating replica set"

# setup user + pass and initialize replica sets
mongosh --host ${m1}:${port} <<EOF
var rootUser = '$MONGO_INITDB_ROOT_USERNAME';
var rootPassword = '$MONGO_INITDB_ROOT_PASSWORD';
var admin = db.getSiblingDB('admin');
admin.auth(rootUser, rootPassword);

rs.initiate()
while (! db.hello().isWritablePrimary ) {print("Db is not writable primary... sleep"); sleep(1000);}

EOF
echo "replica set created"