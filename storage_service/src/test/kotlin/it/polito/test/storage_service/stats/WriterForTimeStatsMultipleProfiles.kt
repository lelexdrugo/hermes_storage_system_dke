package it.polito.test.storage_service.stats

import it.polito.test.storage_service.client.driver.DriverImpl
import it.polito.test.storage_service.client.generator.Generator
import it.polito.test.storage_service.configurations.KafkaConsumerConfig
import it.polito.test.storage_service.configurations.KafkaProperties
import it.polito.test.storage_service.configurations.KafkaTopicConfig
import it.polito.test.storage_service.documents.FileDocument
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random


@Testcontainers
@ActiveProfiles("benchmarkTest")
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class WriterForTimeStatsMultipleProfiles {

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
    @Autowired
    private lateinit var kafkaConsumerConfig: KafkaConsumerConfig
    @Autowired
    lateinit var kafkaTopicConfig: KafkaTopicConfig

    @Value("\${spring.kafka.concurrency-listener}")
    lateinit var numberOfListener: String
    @Value("\${spring.kafka.topics.number-of-partitions}")
    lateinit var numberOfPartition: String

    private val logger = KotlinLogging.logger {}

    val KIBIBYTE = 1024L
    val MEBIBYTE = 1024 * KIBIBYTE

    @Container
    var mongoContainer: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
        .apply {
            this.start();
            System.setProperty("spring.data.mongodb.uri", this.replicaSetUrl)
            println("Mongo url: $replicaSetUrl")
        }

    @Container
    var kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .apply {
            this.start();
            System.setProperty("spring.kafka.bootstrap-servers", this.bootstrapServers)
            println("Spring kafka: $bootstrapServers")
        }

    //Get sul future non funzione. Il broker gestisce la cancellazione schedulandola e comunque aspetta almeno log.
    // log.flush.offset.checkpoint.interval.ms = 60000
    // log.segment.delete.delay.ms = 60000
    // log.flush.start.offset.checkpoint.interval.ms = 60000
    // Poi se auto.create.topics.enable=true; il topic viene ricreato se un consumer fa qualche fetch o qualcosa https://stackoverflow.com/questions/46949188/kafka-topic-is-getting-reappeared-after-10-sec-of-deletion
    // Assciurarsi che la delete sia abilitata nel broker
    @BeforeEach
    fun setUp() {
/*        AdminClient.create(kafkaTopicConfig.kafkaAdmin().configurationProperties).use { adminClient ->
            adminClient.deleteTopics(mutableListOf("fileStorage")).topicNameValues()["fileStorage"]?.get();
            println("Schedulated topic delection" )
            Thread.sleep(120000) //Wait for topic deletion. Retention time 60000
            adminClient.createTopics(kafkaTopicConfig.topicCollection).topicId("fileStorage").get()
            Thread.sleep(5000)
        }*/

        kafkaConsumerConfig.stopListeners()
        mongoDb.db.getCollection("rcs.files").drop()
        mongoDb.db.getCollection("rcs.chunks").drop()
    }

    fun prefillTopic(numberOfFiles: Int, maxDimension: Long, fixedMaxDimension: Boolean, nOfThread: Int){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val files = Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension)
        println("Number of file to generate: ${files.size}")
        val metadata = mutableMapOf("contentType" to "document/docx")
        val threadRatio = 10
//        var nOfThread = numberOfFiles/threadRatio
        //Try to force a bit
//        nOfThread = 12.coerceAtMost(numberOfFiles) //Try this value after some tuning//nOfThread.coerceAtLeast(10).coerceAtMost(17)
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
                "Number of files: $numberOfFiles (generated: ${files.size}), maxDimension: $maxDimension, fixedMaxDimension: $fixedMaxDimension"+" \n" +
                "Number of thread: $nOfThread")
        worker.shutdown() //...
    }


    //execution for a test to check directly the length without contact the server
    fun executeTest(maxDimension: Long, numberOfFiles: Int, fixedMaxDimension: Boolean, nOfThread: Int){
        prefillTopic(numberOfFiles, maxDimension, fixedMaxDimension, nOfThread)

        Assertions.assertFalse(kafkaConsumerConfig.isListenerRunning()) { "The listeners are already running" }
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

        val dataLines: MutableList<String> = mutableListOf()
        dataLines.add("NumberOfFiles MaxDimension FixedMaxDimension NUmberOfTopicPartition NumberOfListenerThreadConsuming TimeToConsumeAndPersistAllTheMessage(ms)")
        dataLines.add("$numberOfFiles $maxDimension $fixedMaxDimension $numberOfPartition $numberOfListener ${timeAfter - timeBefore}")
        dataLines.saveToCsvFile("benchMDT.csv")

        println("Time to consume and persist all the message: ${timeAfter - timeBefore} ms"+"\n" +
                "Number of files: $numberOfFiles (validated ${files.size}, maxDimension: $maxDimension, fixedMaxDimension: $fixedMaxDimension"+" \n" +
                "Number of topic partition: $numberOfPartition, number of listener thread consuming: $numberOfListener")
    }
    //I'm limited from max number of thread to generate: 50
    //Aumentare il numero di file li fa distribuire statisticamente sulle partizioni, ma ogni thread li genera in sequenza.
    //Quindi in realtà in uno stesso slot temporale, sulle partition saranno presenti un numero di file limitato dal numero di thread a generare.
    //Es. con 50 thread, anche se genero 256 file nel test, i producer troveranno prima i primi 50 file, poi altri 50 ecc.
    //In ogni partition questi sono intercalati l'un l'altro, ma quindi con 20 partition e 50 consumer guardando una singola partition vedo file1,file2,file1,file2... quindi su mongo creo sono 50 file. Poi altri 50 ecc
    //Le bulk write sono pompate in questo caso.
    //Dovrei avere o n di thread = n di file, oppure generare più file in uno stesso worker... ma perdo di performance avendo un solo consumer
    //Possiamo ipotizzare di riempire la coda così, fregandocene del risultato e dopo misurare quel tempo per consumare


    /************************************5 partition**********************************************/
    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer5Partition5MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object {
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "5" }
                registry.add("spring.kafka.topics.number-of-partitions") { "5" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "5242880" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 5 partitions/5MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 5 partitions/5MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }

    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer5Partition10MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object{
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "5" }
                registry.add("spring.kafka.topics.number-of-partitions") { "5" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "10485760" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 5 partitions/10MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 5 partitions/10MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }
    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer5Partition20MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object {
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "5" }
                registry.add("spring.kafka.topics.number-of-partitions") { "5" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "20971520" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 5 partitions/20MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 5 partitions/20MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }



    /******************************10 partition********************************************/

    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer10Partition5MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object{
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "10" }
                registry.add("spring.kafka.topics.number-of-partitions") { "10" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "5242880" }
            }
        }

        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 10 partitions/5MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 10 partitions/5MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }

    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer10Partition10MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object{
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "10" }
                registry.add("spring.kafka.topics.number-of-partitions") { "10" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "10485760" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 10 partitions/10MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 10 partitions/10MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }
    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer10Partition20MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object {
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "10" }
                registry.add("spring.kafka.topics.number-of-partitions") { "10" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "20971520" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 10 partitions/20MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 10 partitions/20MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }

    /*************************20 partition*****************************/

    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer20Partition5MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object{
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "20" }
                registry.add("spring.kafka.topics.number-of-partitions") { "20" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "5242880" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 20 partitions/5MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 20 partitions/5MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }


    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer20Partition10MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object{
            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "20" }
                registry.add("spring.kafka.topics.number-of-partitions") { "20" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "10485760" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 20 partitions/10MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 20 partitions/10MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }

    @DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
    class Writer20Partition20MBTest : WriterForTimeStatsMultipleProfiles() {
        companion object {


            @JvmStatic
            @DynamicPropertySource
            fun properties(registry: DynamicPropertyRegistry) {
                registry.add("spring.kafka.concurrency-listener") { "20" }
                registry.add("spring.kafka.topics.number-of-partitions") { "20" }
                registry.add("spring.kafka.consumer.max-partition-fetch-bytes") { "20971520" }
            }
        }
        @DisplayName("50 thread PRODUCING 50 file/100MB; CONSUMING 20 partitions/20MB poll")
        @Test
        fun test50Thread100MB() {
            val numberOfFiles = 50
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 50
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("100 thread PRODUCING 100 file/100MB; CONSUMING 20 partitions/20MB poll")
        @Test
        fun test100Thread100MB() {
            val numberOfFiles = 100
            val maxDimension = 100 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 100
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
    }

}


//Convert a string with space in a string comma separated
fun String.toCsvString(): String {
    return this.split(" ").joinToString(separator = ",")
}

//function to save to a file a list of string through a csv print writer
fun MutableList<String>.saveToCsvFile(fileName: String) {
    val outputfilestream = FileWriter(File(fileName), true)
    val printWriter = PrintWriter(outputfilestream)
    this.forEach { printWriter.println(it.toCsvString()) }
    printWriter.close()
}
