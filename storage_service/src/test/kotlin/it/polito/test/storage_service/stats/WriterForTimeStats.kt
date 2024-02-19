package it.polito.test.storage_service.stats

import it.polito.test.storage_service.client.driver.DriverImpl
import it.polito.test.storage_service.client.generator.Generator
import it.polito.test.storage_service.configurations.KafkaConsumerConfig
import it.polito.test.storage_service.configurations.KafkaProducerConfig
import it.polito.test.storage_service.configurations.KafkaProperties
import it.polito.test.storage_service.dtos.FileResourceBlockDTO
import it.polito.test.storage_service.documents.FileDocument
import mu.KotlinLogging
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.client.exchange
import org.springframework.core.io.ResourceLoader
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.http.*
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

@Testcontainers
@ActiveProfiles("benchmarkTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WriterForTimeStats {
    companion object{
        @Container
        var mongoContainer: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))

        @Container
        var kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1")).apply {
            this.start()
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.mongodb.uri", mongoContainer::getReplicaSetUrl)
            registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers)
        }
    }

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, Message<Any>>
    @Autowired
    private lateinit var kafkaProperties: KafkaProperties

    @Autowired
    private lateinit var restTemplate: TestRestTemplate

    @Autowired
    lateinit var mongoDb: MongoTemplate
    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    private val logger = KotlinLogging.logger {}

    val KIBIBYTE = 1024L
    val MEBIBYTE = 1024 * KIBIBYTE

    @BeforeEach
    fun setUp() {
        kafkaConsumerConfig.stopListeners()
/*        val listeners = kafkaListenerEndpointRegistry.getListenerContainer("unique")!!
        listeners.stop()*/
        mongoDb.db.getCollection("rcs.files").drop()
        mongoDb.db.getCollection("rcs.chunks").drop()
    }

    @Autowired
    lateinit var myProducerConfig: KafkaProducerConfig

    @DisplayName("Prefill topic with large number of file of fixed big dimension")
    fun prefillTopic(numberOfFiles: Int, maxDimension: Long, fixedMaxDimension: Boolean){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val files = Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val threadRatio = 10
        var nOfThread = numberOfFiles/threadRatio
        //Try to force a bit
        nOfThread = 12.coerceAtMost(numberOfFiles) //Try this value after some tuning//nOfThread.coerceAtLeast(10).coerceAtMost(17)
        val worker = Executors.newFixedThreadPool(nOfThread, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                    Thread(null, r, "generator-thread-${counter.incrementAndGet()}")
        })

        //Array of futures
            val results: MutableList<Future<*>> = mutableListOf()
        val timeBefore = System.currentTimeMillis()
        for (file in files) {
                val futureResult = worker.submit {
                    //val kafkaTemplate = KafkaTemplate(myProducerConfig.producerFactory())

                    println("I'm generating the file ${file.key} with kafkaTemplate: $kafkaTemplate")
                    val driverLocal = DriverImpl()
                    val gen = Generator(file.key)
                    var generatedDimension = 0L
                    while (generatedDimension < file.value) {
//                        var pieceDimension = Random.nextLong(maxRequestSize - 100L, (2 * maxRequestSize).toLong())
                        var pieceDimension = Random.nextLong((0.5 * maxRequestSize).toLong(), (1.1 * maxRequestSize).toLong())
                        if (generatedDimension + pieceDimension > file.value) {
                            pieceDimension = file.value - generatedDimension
                        }
                        val content = gen.generate(pieceDimension)
                        driverLocal.writeOnDisk(
                            kafkaTemplate,
                            kafkaProperties.topics.listOfTopics[0],
                            file.key,
                            content,
                            metadata
                        )
                        generatedDimension += pieceDimension
                    }
                    println("Finish to send message for: ${file.key}")
                    kafkaTemplate.producerFactory.closeThreadBoundProducer()
                }
                results.add(futureResult)
            }
            for (future in results) {
                future.get()
            }
        val timeAfter = System.currentTimeMillis()
        println("Time to send all the messages: ${timeAfter - timeBefore} ms"+"\n" +
                "Number of files: $numberOfFiles, maxDimension: $maxDimension, fixedMaxDimension: $fixedMaxDimension"+" \n" +
                "Number of thread: $nOfThread")
            worker.shutdown() //...
    }

    //@Test
    fun `calculate time for consuming message from multiple file of huge size and serving them to a client`(){
        val maxDimension = 10*MEBIBYTE
        val numberOfFiles = 10
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I want to quickly check if the size is the expected one
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()

        val validation = mutableMapOf<String, Thread>()
        for (file in files){
            validation[file.key] = thread(start = true){
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<FileResourceBlockDTO>
                var validatedByte = 0
                //I don't want to validate. I want to quickly check if the size is the expected one
                //val validator = Validator(file.key)
                val totalDimension = file.value
                do {
                    response = restTemplate.exchange(
                        "/files/?filename=${file.key}&offset=$validatedByte",
                        HttpMethod.GET,
                    )
                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        logger.trace("The file: ${file.key} is not yet created")
                        //Creando 100 file, con mono thread, i messaggi vengono mandati sequenzialmente per un unico file.
                        //Quindi lato client verranno consumati più velocemente (ho molti thread), in relazione ai messaggi ricevuti per differenti filename
                        //Quindi validando avrò magari 90/95 thread che loopano su questo primo if (ancora il file non è stato creato) per molto tempo.
                        //Meglio metterli a dormire. Avrebbe molto senso, in questo specifico contesto, usare le coroutine e fare delle delay, così da venire sospesi
                        Thread.sleep(2000)
                        true
                    } else if (response.statusCode == HttpStatus.OK) {
                        if (validatedByte.toLong() != totalDimension) {
                            logger.trace("The file: ${file.key} is not yet complete")
                            validatedByte += response.body!!.data.size

                            assertNotNull(response.body, "The response is null")
                            assertEquals(
                                HttpStatus.OK,
                                response.statusCode,
                                "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                            )
                            assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                            assertEquals(
                                metadata["contentType"],
                                response.body!!.file.metadata["_contentType"],
                                "The field contentType of metadata is not the same"
                            )
                            //Return true if the file is not yet complete
                            validatedByte.toLong() != totalDimension
                            //true
                        } else {
                            logger.info("The file: ${file.key} is complete")
                            false
                        }
                    } else {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
//                    Thread.sleep(1000)
                } while (repeat)
            }
        }

        for (file in files)
            validation[file.key]!!.join()

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message, persist them and present to multiple client by 2MB pieces: ${timeAfter - timeBefore} ms")
    }

    /**
     * Less pressure from clients: check only total length of the file
     */
    //@Test
    fun `calculate time for consuming message from multiple file of huge size`(){
        val maxDimension = 10*MEBIBYTE
        val numberOfFiles = 10
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()

        val validation = mutableMapOf<String, Thread>()
        for (file in files){
            validation[file.key] = thread(start = true){
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<FileResourceBlockDTO>
                val totalDimension = file.value
                do {
                    response = restTemplate.exchange(
                        "/files/?filename=${file.key}",
                        HttpMethod.GET,
                    )
                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        logger.trace("The file: ${file.key} is not yet created")
                        //Creando 100 file, con mono thread, i messaggi vengono mandati sequenzialmente per un unico file.
                        //Quindi lato client verranno consumati più velocemente (ho molti thread), in relazione ai messaggi ricevuti per differenti filename
                        //Quindi validando avrò magari 90/95 thread che loopano su questo primo if (ancora il file non è stato creato) per molto tempo.
                        //Meglio metterli a dormire. Avrebbe molto senso, in questo specifico contesto, usare le coroutine e fare delle delay, così da venire sospesi
                        Thread.sleep(2000)
                        true
                    } else if (response.statusCode == HttpStatus.OK) {
                        if (response.body!!.file.length != totalDimension) {
                            logger.trace("The file: ${file.key} is not yet complete")
                            Thread.sleep(1000)
                            true
                        } else {
                            logger.info("The file: ${file.key} is complete")
                            false
                        }
                    } else {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
//                    Thread.sleep(1000)
                } while (repeat)
            }
        }

        for (file in files)
            validation[file.key]!!.join()

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }

    /**
     * Less pressure from clients: check only total length of the file
     */
    //@Test
    fun `calculate time for consuming message from huge amount of file of small size`(){
        val maxDimension = 1*MEBIBYTE
        val numberOfFiles = 1000
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()


        val nOfValidatorWorker = 100.coerceAtMost(numberOfFiles)
        val workerValidator = Executors.newFixedThreadPool(nOfValidatorWorker, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        val validation = mutableMapOf<String, Future<*>>()
        for (file in files){
            validation[file.key] = workerValidator.submit{
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<FileResourceBlockDTO>
                val totalDimension = file.value
                do {
                    response = restTemplate.exchange(
                        "/files/?filename=${file.key}",
                        HttpMethod.GET,
                    )
                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        logger.trace("The file: ${file.key} is not yet created")
                        //Creando 100 file, con mono thread, i messaggi vengono mandati sequenzialmente per un unico file.
                        //Quindi lato client verranno consumati più velocemente (ho molti thread), in relazione ai messaggi ricevuti per differenti filename
                        //Quindi validando avrò magari 90/95 thread che loopano su questo primo if (ancora il file non è stato creato) per molto tempo.
                        //Meglio metterli a dormire. Avrebbe molto senso, in questo specifico contesto, usare le coroutine e fare delle delay, così da venire sospesi
                        Thread.sleep(2000)
                        true
                    } else if (response.statusCode == HttpStatus.OK) {
                        if (response.body!!.file.length != totalDimension) {
                            logger.trace("The file: ${file.key} is not yet complete")
                            Thread.sleep(1000)
                            true
                        } else {
                            logger.info("The file: ${file.key} is complete")
                            false
                        }
                    } else {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
//                    Thread.sleep(1000)
                } while (repeat)
            }
        }

        for (file in files)
            validation[file.key]!!.get()

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }

    @Autowired
    private lateinit var kafkaConsumerConfig: KafkaConsumerConfig

    //version of a test to check directly the length without contact the server
    @Test
    fun `calculate time for consuming message from huge amount of file of small size, checking directly final size`(){
        val maxDimension = 100*MEBIBYTE
        val numberOfFiles =256
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val threadRatio = 10

        val validation = mutableMapOf<String, Future<*>>()
        val nOfValidator = 100.coerceAtMost(numberOfFiles)//(numberOfFiles/threadRatio).coerceAtLeast(50).coerceAtMost(numberOfFiles)
        val workerValidator = Executors.newFixedThreadPool(nOfValidator, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        for (file in files){
            validation[file.key] = workerValidator.submit{
                println("I'm validating the file ${file.key}")
                val totalDimension = file.value
                do {
                    var fileRetrieved: FileDocument? = null
                    try {
                        val query = Query()
                        query.addCriteria(Criteria.where("filename").`is`(file.key).and("length").`is`(totalDimension))
                        fileRetrieved = mongoTemplate.find(query, FileDocument::class.java).firstOrNull()
                    }
                    catch (e: DataAccessResourceFailureException){
                        logger.warn("DataAccessResourceFailureException. Probably MongoTimeoutException. Error while retrieving the file ${file.key}")
                    }
                    finally {
                        if(fileRetrieved == null)
                            Thread.sleep(3000)
                    }
                } while (fileRetrieved == null)
                println("The file ${file.key} is complete")
            }
        }

        for (file in files)
            try {
                validation[file.key]!!.get()
            }
            catch (e: Exception){
                logger.error("Error while validating the file ${file.key}. Error: ${e.message}")
                throw e
            }

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }


    //@Test
    fun `fill topic consuming with test method, checking directly final size`(){
        val maxDimension = 1024*MEBIBYTE
        val numberOfFiles = 1
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension)

        do {
            val query = Query()
            query.addCriteria(Criteria.where("filename").`is`("finito"))
            val fileRetrieved = mongoTemplate.find(query, FileDocument::class.java).firstOrNull()
            if(fileRetrieved == null)
                Thread.sleep(3000)
        } while (fileRetrieved == null)

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }


    @Autowired
    private lateinit var resourceLoader: ResourceLoader
    @Autowired
    lateinit var gridFs: GridFsTemplate

    @OptIn(ExperimentalTime::class)
    //@Test
    fun `inject my video with gridFS`() {
        val filename = "hugeFileForTest2"
        val inputUri = resourceLoader.getResource("classpath:hugeVideo.mp4").uri

        val inputStream = File(inputUri).inputStream()
        val timeBefore = System.currentTimeMillis()
        val time = measureTime { gridFs.store(inputStream, filename, "video/mp4") }

        println("Time to store the file: ${System.currentTimeMillis() - timeBefore}")
        println("Time to store the file with measureBlock: $time")
    }



    @Test
    fun `2calculate time for consuming message from huge amount of file of small size, checking directly final size`(){
        val maxDimension = 25*MEBIBYTE
        val numberOfFiles =256
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val threadRatio = 10

        val validation = mutableMapOf<String, Future<*>>()
        val nOfValidator = 100.coerceAtMost(numberOfFiles)//(numberOfFiles/threadRatio).coerceAtLeast(50).coerceAtMost(numberOfFiles)
        val workerValidator = Executors.newFixedThreadPool(nOfValidator, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        for (file in files){
            validation[file.key] = workerValidator.submit{
                println("I'm validating the file ${file.key}")
                val totalDimension = file.value
                do {
                    var fileRetrieved: FileDocument? = null
                    try {
                        val query = Query()
                        query.addCriteria(Criteria.where("filename").`is`(file.key).and("length").`is`(totalDimension))
                        fileRetrieved = mongoTemplate.find(query, FileDocument::class.java).firstOrNull()
                    }
                    catch (e: DataAccessResourceFailureException){
                        logger.warn("DataAccessResourceFailureException. Probably MongoTimeoutException. Error while retrieving the file ${file.key}")
                    }
                    finally {
                        if(fileRetrieved == null)
                            Thread.sleep(3000)
                    }
                } while (fileRetrieved == null)
                println("The file ${file.key} is complete")
            }
        }

        for (file in files)
            try {
                validation[file.key]!!.get()
            }
            catch (e: Exception){
                logger.error("Error while validating the file ${file.key}. Error: ${e.message}")
                throw e
            }

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }


    @Test
    fun `3calculate time for consuming message from huge amount of file of small size, checking directly final size`(){
        val maxDimension = 100*MEBIBYTE
        val numberOfFiles =250
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val threadRatio = 10

        val validation = mutableMapOf<String, Future<*>>()
        val nOfValidator = 100.coerceAtMost(numberOfFiles)//(numberOfFiles/threadRatio).coerceAtLeast(50).coerceAtMost(numberOfFiles)
        val workerValidator = Executors.newFixedThreadPool(nOfValidator, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        for (file in files){
            validation[file.key] = workerValidator.submit{
                println("I'm validating the file ${file.key}")
                val totalDimension = file.value
                do {
                    var fileRetrieved: FileDocument? = null
                    try {
                        val query = Query()
                        query.addCriteria(Criteria.where("filename").`is`(file.key).and("length").`is`(totalDimension))
                        fileRetrieved = mongoTemplate.find(query, FileDocument::class.java).firstOrNull()
                    }
                    catch (e: DataAccessResourceFailureException){
                        logger.warn("DataAccessResourceFailureException. Probably MongoTimeoutException. Error while retrieving the file ${file.key}")
                    }
                    finally {
                        if(fileRetrieved == null)
                            Thread.sleep(3000)
                    }
                } while (fileRetrieved == null)
                println("The file ${file.key} is complete")
            }
        }

        for (file in files)
            try {
                validation[file.key]!!.get()
            }
            catch (e: Exception){
                logger.error("Error while validating the file ${file.key}. Error: ${e.message}")
                throw e
            }

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }


    @Test
    fun `4calculate time for consuming message from huge amount of file of small size, checking directly final size`(){
        val maxDimension = 10*MEBIBYTE
        val numberOfFiles =250
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val threadRatio = 10

        val validation = mutableMapOf<String, Future<*>>()
        val nOfValidator = 100.coerceAtMost(numberOfFiles)//(numberOfFiles/threadRatio).coerceAtLeast(50).coerceAtMost(numberOfFiles)
        val workerValidator = Executors.newFixedThreadPool(nOfValidator, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        for (file in files){
            validation[file.key] = workerValidator.submit{
                println("I'm validating the file ${file.key}")
                val totalDimension = file.value
                do {
                    var fileRetrieved: FileDocument? = null
                    try {
                        val query = Query()
                        query.addCriteria(Criteria.where("filename").`is`(file.key).and("length").`is`(totalDimension))
                        fileRetrieved = mongoTemplate.find(query, FileDocument::class.java).firstOrNull()
                    }
                    catch (e: DataAccessResourceFailureException){
                        logger.warn("DataAccessResourceFailureException. Probably MongoTimeoutException. Error while retrieving the file ${file.key}")
                    }
                    finally {
                        if(fileRetrieved == null)
                            Thread.sleep(3000)
                    }
                } while (fileRetrieved == null)
                println("The file ${file.key} is complete")
            }
        }

        for (file in files)
            try {
                validation[file.key]!!.get()
            }
            catch (e: Exception){
                logger.error("Error while validating the file ${file.key}. Error: ${e.message}")
                throw e
            }

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }

    @Test
    fun `5calculate time for consuming message from huge amount of file of small size, checking directly final size`(){
        val maxDimension = 5*MEBIBYTE*MEBIBYTE
        val numberOfFiles =1
        val fixedMaxDimension = true

        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension)

        assertFalse(kafkaConsumerConfig.isListenerRunning()){ "The listeners are already running" }
        kafkaConsumerConfig.startListener()

        val timeBefore = System.currentTimeMillis()
        //I don't use a validator
        //I use client only to check if all the message are stored (so the length in the db is the expected one)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val threadRatio = 10

        val validation = mutableMapOf<String, Future<*>>()
        val nOfValidator = 100.coerceAtMost(numberOfFiles)//(numberOfFiles/threadRatio).coerceAtLeast(50).coerceAtMost(numberOfFiles)
        val workerValidator = Executors.newFixedThreadPool(nOfValidator, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        for (file in files){
            validation[file.key] = workerValidator.submit{
                println("I'm validating the file ${file.key}")
                val totalDimension = file.value
                do {
                    var fileRetrieved: FileDocument? = null
                    try {
                        val query = Query()
                        query.addCriteria(Criteria.where("filename").`is`(file.key).and("length").`is`(totalDimension))
                        fileRetrieved = mongoTemplate.find(query, FileDocument::class.java).firstOrNull()
                    }
                    catch (e: DataAccessResourceFailureException){
                        logger.warn("DataAccessResourceFailureException. Probably MongoTimeoutException. Error while retrieving the file ${file.key}")
                    }
                    finally {
                        if(fileRetrieved == null)
                            Thread.sleep(3000)
                    }
                } while (fileRetrieved == null)
                println("The file ${file.key} is complete")
            }
        }

        for (file in files)
            try {
                validation[file.key]!!.get()
            }
            catch (e: Exception){
                logger.error("Error while validating the file ${file.key}. Error: ${e.message}")
                throw e
            }

        val timeAfter = System.currentTimeMillis()
        logger.info("Service time to consume all the message and persist them: ${timeAfter - timeBefore} ms")
    }
}