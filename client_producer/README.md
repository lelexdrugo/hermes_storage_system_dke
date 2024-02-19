To launch ClientProducer from cli:
    - Use gradlew run task
        or
    - gradlew bootJar in \storage_service\client_producer
    -   cd scripts
        multipleClients.bat [numberOfParallelFile] [fileDimension]

    To run in silent mode
    - wscript.exe invis.vbs multipleClients.bat %*arguments

Alternatively you can run also script for single generation
    - cmd singleClient.bat [filename] [filedimension]

