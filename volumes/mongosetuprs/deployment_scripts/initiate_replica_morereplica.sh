#!/bin/bash

m1=mongo1
port=30001
echo "Starting replica set initialize"
until mongosh --host ${m1}:${port} --eval "print(\"waited for connection\")"
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

rs.initiate(
  {
    _id : 'rs0',
    members: [
      { _id : 0, host : "mongo1:30001" },
      { _id : 1, host : "mongo2:30002" },
      { _id : 2, host : "mongo3:30003" }
    ]
  }
)

EOF
echo "replica set created"