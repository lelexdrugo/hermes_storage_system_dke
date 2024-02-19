#!/bin/bash

result=$(mongosh --quiet --eval "db.hello().isWritablePrimary")
echo "$result"
if [ "$result" == "true" ]; then
    echo "healthcheck passed"
    exit 0
else
    echo "healthcheck failed"
    exit 1
fi


#m1=mongo1
#port=27017
#echo "Starting replica set healthcheck"
#
#until mongosh --host mongo1:27017 --username admin-user --password admin-password --eval "print(\"waited for connection\")"
#do
#    sleep 2
#done
#echo "Connection finished"
## setup user + pass and initialize replica sets
#if [ "$(mongosh --host mongo1:27017 --username admin-user --password admin-password --eval "db.hello().isWritablePrimary")" == "true" ]; then
#    echo "healthcheck passed"
#    exit 0
#else
#    echo "healthcheck failed"
#    exit 1
#fi

#function check {
#  STATUS=\`curl -s --unix-socket /var/run/docker.sock http:/v1.24/containers/postgres/json | python -c 'import sys, json; print json.load('sys.stdin')["State"]["Health"]["Status"]'\`
#
#  if [ "$STATUS" = "healthy" ]; then
#    return 0
#  fi
#  return 1
#}
#
#until check; do
#  echo "Waiting for postgres to be ready"
#  sleep 5
#done
#
#echo "Postgres ready"