package it.polito.test.storage_service.stats

import it.polito.test.storage_service.client.generator.Generator
import it.polito.test.storage_service.configurations.KafkaProperties
import it.polito.test.storage_service.client.driver.DriverImpl
import it.polito.test.storage_service.dtos.FileResourceBlockDTO
import mu.KotlinLogging
import org.bson.Document
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.client.exchange
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.http.*
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import kotlin.random.Random

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WriterForStats {
    companion object{
//        @Container
//        var mongoContainer: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))

        @Container
        var kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1")).apply {
            this.start()
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
//            registry.add("spring.data.mongodb.uri", mongoContainer::getReplicaSetUrl)
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

    val driver = DriverImpl()

    val filename = Random.nextLong().toString()

    private val logger = KotlinLogging.logger {}

    val KIBIBYTE = 1024L
    val MEBIBYTE = 1024 * KIBIBYTE

    @BeforeEach
    fun setUp() {
        mongoDb.db.getCollection("rcs.files").drop()
        mongoDb.db.getCollection("rcs.chunks").drop()
    }
    data class Stats(val resultsStats: Document, val fileCollectionStats: Document, val chunksCollectionStats: Document)

    fun statsBefore(): Stats {
        val resultStats= mongoDb.db.runCommand(Document(mapOf("dbstats" to 1, "scale" to 1048576)))
        val fileCollectionStats= mongoDb.db.runCommand(Document("collStats", "rcs.files"))
        val chunksCollectionStats= mongoDb.db.runCommand(Document("collStats", "rcs.chunks"))
/*        logger.info { "**** BEFORE ****"}
        //logger.info { "DB stats in MB: $resultStats" }
        logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStats["fsUsedSize"] as Double}" }
        //logger.info { "File collection stats: $fileCollectionStats" }
        //logger.info { "Chunks collection stats: $chunksCollectionStats" }
        logger.info { "The total size of all indexes"}
        logger.info { "File collection totalIndexSize: ${fileCollectionStats["totalIndexSize"]}"}
        logger.info { "Chunks collection totalIndexSize: ${chunksCollectionStats["totalIndexSize"]}"}*/
        return Stats(resultStats, fileCollectionStats, chunksCollectionStats)
    }

    fun statsAfter(): Stats {
        val resultStatsAfter= mongoDb.db.runCommand(Document(mapOf("dbstats" to 1, "scale" to 1048576)))
        val fileCollectionStatsAfter= mongoDb.db.runCommand(Document("collStats", "rcs.files"))
        val chunksCollectionStatsAfter= mongoDb.db.runCommand(Document("collStats", "rcs.chunks"))

/*        logger.info { "**** AFTER ****"}
        //logger.info { "DB stats: $resultStatsAfter" }
        logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStatsAfter["fsUsedSize"] as Double}" }
        //logger.info { "File collection stats: $fileCollectionStatsAfter" }
        //logger.info { "Chunks collection stats: $chunksCollectionStatsAfter" }
        logger.info { "The total size of all indexes"}
        logger.info { "File collection totalIndexSize: ${fileCollectionStatsAfter["totalIndexSize"]}"}
        logger.info { "Chunks collection totalIndexSize: ${chunksCollectionStatsAfter["totalIndexSize"]}"}*/

        return Stats(resultStatsAfter, fileCollectionStatsAfter, chunksCollectionStatsAfter)
    }

    fun calculateDiff(statsBefore: Stats, statsAfter: Stats){
        /**
         * About fsUsedSize and fsTotalSize
         *  these are all about the filesystem where the database is stored.
         *  These are used to get an idea of how much a database can grow
         */
        logger.info { "**** BEFORE ****"}
        //logger.info { "DB stats in MB: $resultStats" }
        logger.info { "fsUsedSize. Total size of all disk space in use on the filesystem where MongoDB stores data, with scale=1048576 (i.e. 1 MBi)." }
        logger.info { "fsUsedSize, with scale=1048576 (i.e. 1 MBi): ${statsBefore.resultsStats["fsUsedSize"] as Double}" }
        logger.info { "dataSize. Total size of the uncompressed data held in the database, with scale=1048576 (i.e. 1 MBi)." }
        logger.info { "dataSize, with scale=1048576 (i.e. 1 MBi): ${statsBefore.resultsStats["dataSize"] as Double}" }
        //logger.info { "File collection stats: $fileCollectionStats" }
        //logger.info { "Chunks collection stats: $chunksCollectionStats" }
        logger.info { "The total size of all indexes"}
        logger.info { "File collection totalIndexSize: ${statsBefore.fileCollectionStats["totalIndexSize"]}"}
        logger.info { "Chunks collection totalIndexSize: ${statsBefore.chunksCollectionStats["totalIndexSize"]}"}

        logger.info { "**** AFTER ****"}
        //logger.info { "DB stats: $resultStatsAfter" }
        logger.info { "fsUsedSize. Total size of all disk space in use on the filesystem where MongoDB stores data, with scale=1048576 (i.e. 1 MBi)." }
        logger.info { "fsUsedSize with scale=1048576 (i.e. 1 MBi): ${statsAfter.resultsStats["fsUsedSize"] as Double}" }
        logger.info { "dataSize. Total size of the uncompressed data held in the database, with scale=1048576 (i.e. 1 MBi)." }
        logger.info { "dataSize, with scale=1048576 (i.e. 1 MBi): ${statsAfter.resultsStats["dataSize"] as Double}" }
        //logger.info { "File collection stats: $fileCollectionStatsAfter" }
        //logger.info { "Chunks collection stats: $chunksCollectionStatsAfter" }
        logger.info { "The total size of all indexes"}
        logger.info { "File collection totalIndexSize: ${statsAfter.fileCollectionStats["totalIndexSize"]}"}
        logger.info { "Chunks collection totalIndexSize: ${statsAfter.chunksCollectionStats["totalIndexSize"]}"}


        logger.info { "**** DIFFERENCE ****"}
        logger.info { "(DIFFERENCE) fsUsedSize. Total size of all disk space in use on the filesystem where MongoDB stores data, with scale=1048576 (i.e. 1 MBi)." }
        logger.info { "(DIFFERENCE) fsUsedSize, with scale=1048576 (i.e. 1 MBi): ${(statsAfter.resultsStats["fsUsedSize"] as Double) - (statsBefore.resultsStats["fsUsedSize"] as Double)}" }
        logger.info { "(DIFFERENCE) dataSize. Total size of the uncompressed data held in the database, with scale=1048576 (i.e. 1 MBi)." }
        logger.info { "(DIFFERENCE) dataSize, with scale=1048576 (i.e. 1 MBi): ${(statsAfter.resultsStats["dataSize"] as Double) - (statsBefore.resultsStats["dataSize"] as Double)}" }
        logger.info { "(DIFFERENCE) Below the total uncompressed size in memory of all records in a collection. The size does not include the size of any indexes associated with the collection, which the totalIndexSize field reports."}
        logger.info { "(DIFFERENCE) File collection size (Byte): ${statsAfter.fileCollectionStats["size"] as Int - statsBefore.fileCollectionStats["size"] as Int}" }
        logger.info { "(DIFFERENCE) Chunks collection size (Byte): ${statsAfter.chunksCollectionStats["size"] as Int - statsBefore.chunksCollectionStats["size"] as Int}" }
        logger.info { "(DIFFERENCE) Below the total size of all indexes:"}
        logger.info { "(DIFFERENCE) File collection totalIndexSize (Byte): ${statsAfter.fileCollectionStats["totalIndexSize"] as Int - statsBefore.fileCollectionStats["totalIndexSize"] as Int}" }
        logger.info { "(DIFFERENCE) Chunks collection totalIndexSize (Byte): ${statsAfter.chunksCollectionStats["totalIndexSize"] as Int - statsBefore.chunksCollectionStats["totalIndexSize"] as Int}" }
    }

    @Test
    fun `single file created with single write`(){
        val statsBefore = statsBefore()
        val totalDimension = 1* MEBIBYTE
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")

        val content = generator.generate(totalDimension)
        driver.writeOnDisk(
            kafkaTemplate,
            kafkaProperties.topics.listOfTopics[0],
            filename,
            content,
            metadata)

        var repeat = true
        var response: ResponseEntity<FileResourceBlockDTO>
        while(repeat){
            response = restTemplate.exchange(
                "/files/?filename=$filename&blockSize=1",
                HttpMethod.GET,
            )
            when (response.statusCode){
                HttpStatus.NOT_FOUND -> {
                    logger.info("The file is not yet created")
                    repeat = true
                }
                HttpStatus.OK -> {
                    repeat = if (response.body!!.file.length != totalDimension) {
                        logger.info("The file is not yet complete")
                        true
                    } else {
                        logger.info("The file is complete")
                        false
                    }
                }
                else -> {
                    logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                    repeat = true
                }
            }
            Thread.sleep(1000)
        }
        val statsAfter = statsAfter()
        calculateDiff(statsBefore, statsAfter)
    }

    @Test
    fun `single file created with multiple write`(){
        val statsBefore = statsBefore()
        val numberOfWrite = KIBIBYTE
        val dimension = MEBIBYTE
        val totalDimension = numberOfWrite * dimension
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")
        for (i in 1..numberOfWrite){
            val content = generator.generate(dimension)
            driver.writeOnDisk(
                kafkaTemplate,
                kafkaProperties.topics.listOfTopics[0],
                filename,
                content,
                metadata)
        }
        var repeat = true
        var response: ResponseEntity<FileResourceBlockDTO>
        while(repeat){
            response = restTemplate.exchange(
                "/files/?filename=$filename&blockSize=1",
                HttpMethod.GET,
            )
            when (response.statusCode){
                HttpStatus.NOT_FOUND -> {
                    logger.info("The file is not yet created")
                    repeat = true
                }
                HttpStatus.OK -> {
                    repeat = if (response.body!!.file.length != totalDimension) {
                        logger.info("The file is not yet complete")
                        true
                    } else {
                        logger.info("The file is complete")
                        false
                    }
                }
                else -> {
                    logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                    repeat = true
                }
            }
            Thread.sleep(1000)
        }
        val statsAfter = statsAfter()
        calculateDiff(statsBefore, statsAfter)
    }

    @Test
    fun `multiple file created with single write`() {
        val statsBefore = statsBefore()

        val numberOfFiles = 100
        val maxDimension = 5 * MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        for (file in files) {
            //logger.info("I'm generating the file ${file.key}")
            val gen = Generator(file.key)
            val content = gen.generate(file.value)
            driver.writeOnDisk(
                kafkaTemplate,
                kafkaProperties.topics.listOfTopics[0],
                file.key,
                content,
                metadata
            )
        }

        for (file in files) {
            var repeat = true
            var response: ResponseEntity<FileResourceBlockDTO>
            while (repeat) {
                response = restTemplate.exchange(
                    "/files/?filename=${file.key}&blockSize=1",
                    HttpMethod.GET,
                )
                when (response.statusCode) {
                    HttpStatus.NOT_FOUND -> {
                        //logger.info("The file is not yet created")
                        repeat = true
                    }

                    HttpStatus.OK -> {
                        repeat = if (response.body!!.file.length != maxDimension) {
                            //logger.info("The file is not yet complete")
                            true
                        } else {
                            //logger.info("The file is complete")
                            false
                        }
                    }

                    else -> {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        repeat = true
                    }
                }
                Thread.sleep(1000)
            }
        }
        val statsAfter = statsAfter()
        calculateDiff(statsBefore, statsAfter)
    }
}