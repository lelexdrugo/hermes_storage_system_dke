@ECHO OFF
set filename=%1
set dimension=%2
cd ../build/libs
java -jar client_producer-0.0.1-SNAPSHOT.jar it.rcs.client_producer.ClientProducerApplication single %filename% %dimension%