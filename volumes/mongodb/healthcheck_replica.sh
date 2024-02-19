#!/bin/bash

positiveResult=0
attempt=0

while [ $attempt -lt 50 ]
do
    attempt=$(($attempt + 1))
    # result1=$(mongosh "mongodb://mongo1:30001/?directConnection=true" --quiet --eval "db.hello().isWritablePrimary")
    # result2=$(mongosh "mongodb://mongo2:30002/?directConnection=true" --quiet --eval "db.hello().isWritablePrimary")
    # result3=$(mongosh "mongodb://mongo3:30003/?directConnection=true" --quiet --eval "db.hello().isWritablePrimary")
    # echo "$result1"
    # if [ "$result1" == "true" ] || [ "$result2" == "true" ] || [ "$result3" == "true" ]; then
    result=$(mongosh "mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=rs0" --quiet --eval "db.hello().isWritablePrimary")
    echo "$result"
    if [ "$result" == "true" ]; then
        positiveResult=$(($positiveResult + 1))
        if [ $positiveResult -eq 3 ]; then
            echo "healthcheck passed"
            exit 0
        fi
    fi
done
echo "healthcheck failed"
exit 1


# result=$(mongosh --quiet --eval "db.hello().isWritablePrimary")
# echo "$result"
# if [ "$result" == "true" ]; then
#     echo "healthcheck passed"
#     exit 0
# else
#     echo "healthcheck failed"
#     exit 1
# fi


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