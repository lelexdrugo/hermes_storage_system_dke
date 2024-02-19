package it.polito.test.storage_service.stats

import it.polito.test.storage_service.client.driver.DriverImpl
import it.polito.test.storage_service.client.generator.Generator
import it.polito.test.storage_service.configurations.KafkaConsumerConfig
import it.polito.test.storage_service.configurations.KafkaProperties
import it.polito.test.storage_service.configurations.KafkaTopicConfig
import it.polito.test.storage_service.documents.FileDocument
import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.junit.jupiter.api.*
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
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

@Testcontainers
@ActiveProfiles("benchmarkTest")
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD) //Find a way to destroy topic between each test
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WriterForTimeStatsMultipleProfiles2 {

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
    lateinit var kafkaConsumerConfig: KafkaConsumerConfig
    @Autowired
    lateinit var kafkaTopicConfig: KafkaTopicConfig

    @Value("\${spring.kafka.concurrency-listener}")
    lateinit var numberOfListener: String
    @Value("\${spring.kafka.topics.number-of-partitions}")
    lateinit var numberOfPartition: String

    private val logger = KotlinLogging.logger {}

    val KIBIBYTE = 1024L
    val MEBIBYTE = 1024 * KIBIBYTE



    //Get sul future non funzione. Il broker gestisce la cancellazione schedulandola e comunque aspetta almeno log.
    // log.flush.offset.checkpoint.interval.ms = 60000
    // log.segment.delete.delay.ms = 60000
    // log.flush.start.offset.checkpoint.interval.ms = 60000
    // Poi se auto.create.topics.enable=true; il topic viene ricreato se un consumer fa qualche fetch o qualcosa https://stackoverflow.com/questions/46949188/kafka-topic-is-getting-reappeared-after-10-sec-of-deletion
    // Assciurarsi che la delete sia abilitata nel broker
    //@BeforeEach
    fun setUp() {
        println("BEFORE EACH of abstract class")

        AdminClient.create(kafkaTopicConfig.kafkaAdmin().configurationProperties).use { adminClient ->
            adminClient.deleteTopics(mutableListOf("fileStorage")).topicNameValues()["fileStorage"]?.get();
            println("Schedulated topic delection" )
            Thread.sleep(61000)
            //Thread.sleep(120000) //Wait for topic deletion. Retention time 60000
            adminClient.createTopics(kafkaTopicConfig.topicCollection).topicId("fileStorage").get()
        }

        kafkaConsumerConfig.stopListeners()

        mongoDb.db.getCollection("rcs.files").drop()
        mongoDb.db.getCollection("rcs.chunks").drop()

    }

    fun prefillTopic(numberOfFiles: Int, maxDimension: Long, fixedMaxDimension: Boolean, nOfThread: Int){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val files = Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension)
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
                "Number of files: $numberOfFiles, maxDimension: $maxDimension, fixedMaxDimension: $fixedMaxDimension"+" \n" +
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

        println("Time to consume and persist all the message: ${timeAfter - timeBefore} ms"+"\n" +
                "Number of files: $numberOfFiles, maxDimension: $maxDimension, fixedMaxDimension: $fixedMaxDimension"+" \n" +
                "Number of topic partition: $numberOfPartition, number of listener thread consuming: $numberOfListener")
    }

    /************************************5 partition**********************************************/

        @Container
        var mongoContainer: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
            .apply {
                this.start();
                System.setProperty("spring.data.mongodb.uri", this.replicaSetUrl)}

        @Container
        var kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .apply {
                this.start();
                System.setProperty("spring.kafka.bootstrap-servers", this.bootstrapServers)
            }
    @Value("\${spring.kafka.bootstrap-servers}")
    lateinit var bootstrapServers: String

    @Value("\${spring.data.mongodb.uri}")
    lateinit var mongoUri: String


        @BeforeEach
        fun myBeforeEach() {
            println("Before each")
//            kafkaContainer.start()
//            mongoContainer.start()
/*            println("Mongo url: ${mongoContainer.replicaSetUrl}")
            println("Kafka url: ${kafkaContainer.bootstrapServers}")*/
            println("Mongo url from prop: $mongoUri")
            println("Kafka url from prop: $bootstrapServers")

/*            System.setProperty("spring.data.mongodb.uri", mongoContainer.replicaSetUrl)
            System.setProperty("spring.kafka.bootstrap-servers", kafkaContainer.bootstrapServers)
            System.setProperty("spring.kafka.concurrency-listener", "5")
            System.setProperty("spring.kafka.topics.number-of-partitions", "5")
            System.setProperty("spring.kafka.consumer.max-partition-fetch-bytes", "5242880")*/
            /*            System.setProperty("spring.kafka.concurrency-listener", "5")
                        System.setProperty("spring.kafka.topics.number-of-partitions", "5")
                        System.setProperty("spring.kafka.consumer.max-partition-fetch-bytes", "5242880")*/

            AdminClient.create(kafkaTopicConfig.kafkaAdmin().configurationProperties).use { adminClient ->
                adminClient.deleteTopics(mutableListOf("fileStorage")).topicNameValues()["fileStorage"]?.get();
                println("Schedulated topic delection" )
                Thread.sleep(5000)
                //Thread.sleep(120000) //Wait for topic deletion. Retention time 60000
                adminClient.createTopics(kafkaTopicConfig.topicCollection).topicId("fileStorage").get()
            }

            kafkaConsumerConfig.stopListeners()

            mongoDb.db.getCollection("rcs.files").drop()
            mongoDb.db.getCollection("rcs.chunks").drop()
        }
        @AfterEach
        fun myafterEach() {
            println("After each")
//            kafkaContainer.stop()
//            mongoContainer.stop()
/*            kafkaContainer.start()
            mongoContainer.start()
            System.setProperty("spring.data.mongodb.uri", mongoContainer.replicaSetUrl)
            System.setProperty("spring.kafka.bootstrap-servers", kafkaContainer.bootstrapServers)*/
            Thread.sleep(10000)
        }



        @Test
        fun test1(){
            println("Inside test1")
        }

        @Test
        fun test2(){
            println("Inside test2")
        }

        @DisplayName("5 thread PRODUCING 256 file/100MB; CONSUMING 5 partitions/5MB poll")
        @Test
        fun test5Thread256() {
            val numberOfFiles = 15
            val maxDimension = 5 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 5
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
        @DisplayName("10 thread PRODUCING 256 file/100MB; CONSUMING 5 partitions/5MB poll")
        @Test
        fun test10Thread256() {
            val numberOfFiles = 15
            val maxDimension = 5 * MEBIBYTE
            val fixedMaxDimension = true
            val nOfThread = 10
            executeTest(maxDimension, numberOfFiles, fixedMaxDimension, nOfThread)
        }
}
