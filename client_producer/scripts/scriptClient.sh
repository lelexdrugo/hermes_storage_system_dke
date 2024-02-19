#!/bin/sh

myIndex=1
java -jar client_producer-0.0.1-SNAPSHOT.jar it.rcs.client_producer.ClientProducerApplication single file$index 10485760
