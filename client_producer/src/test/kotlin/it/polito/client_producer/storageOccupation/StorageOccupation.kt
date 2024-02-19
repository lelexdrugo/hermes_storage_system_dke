package it.polito.client_producer.storageOccupation

import it.polito.client_producer.driver.DriverImpl
import mu.KotlinLogging
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*

@SpringBootTest
@Testcontainers
class StorageOccupation {
/*    companion object {
        @Container
        var container: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
            .withExposedPorts(27017)
        //Without port exposed, sometimes
        // onditionTimeoutException GenericContainer expected the predicate to return <true> but it returned <false> for input of <InspectContainerResponse


        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.mongodb.uri", container::getReplicaSetUrl)
        }

    }*/

    private val logger = KotlinLogging.logger {}

/*    @Autowired
    lateinit var gridFs: GridFsTemplate

    @Autowired
    lateinit var mongoDb: MongoTemplate*/

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, Message<Any>>
    private val driver = DriverImpl()

    val CHUNK_SIZE = 261120
    val KIBIBYTE = 1
    val MEBIBYTE = 1024 * KIBIBYTE
    val GIBIBYTE = 1024 * MEBIBYTE
    val CONTENT_SIZE = 10 * CHUNK_SIZE

    //Don't change it. Test are meaningful with this value
    val INITIAL_FILE_SIZE = 0
    //Generate before to speedUp
    val contentByteArrayMB = ByteArray(MEBIBYTE).also { Random().nextBytes(it) }

    val filename = "testFile2"
    val contentType = "text/plain"
    val defaultOffset = -1L

    //Each test start with an empty existing file
    @BeforeEach
    fun setUp() {
        //mongoDb.db.drop()

        driver.writeOnDisk(kafkaTemplate, "fileStorage", filename, byteArrayOf(), mutableMapOf("contentType" to "test"))
        logger.info { "File created: $filename" }
    }

    @AfterEach
    fun tearDownDb() {
        /*        fileRepo.deleteAll()
                chunkRepo.deleteAll()*/
    }

    @Test
    fun `Update a file up to 1 MB`() {
        val contentByteArray = ByteArray(MEBIBYTE).also { Random().nextBytes(it) }
        driver.writeOnDisk(kafkaTemplate, "fileStorage", filename, contentByteArray, mutableMapOf("contentType" to "test"))
        logger.info { "File updated: $filename" }
    }

/*
    @Test
    fun `Update a file up to 1 GB`(){
        //Call write on disk 1024 times to reach 1 GB
        val resultStats= mongoDb.db.runCommand(Document(mapOf("dbstats" to 1, "scale" to 1048576)))
        val fileCollectionStats= mongoDb.db.runCommand(Document("collStats", "rcs.files"))
        val chunksCollectionStats= mongoDb.db.runCommand(Document("collStats", "rcs.chunks"))
        logger.info { "**** BEFORE ****"}
        //logger.info { "DB stats in MB: $resultStats" }
        logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStats["fsUsedSize"] as Int}" }
        //logger.info { "File collection stats: $fileCollectionStats" }
        //logger.info { "Chunks collection stats: $chunksCollectionStats" }
        logger.info { "The total size of all indexes"}
        logger.info { "File collection totalIndexSize: ${fileCollectionStats["totalIndexSize"]}"}
        logger.info { "Chunks collection totalIndexSize: ${chunksCollectionStats["totalIndexSize"]}"}


        for (i in 1..1024) {
            driver.writeOnDisk(kafkaTemplate, "fileStorage", filename, contentByteArrayMB, mutableMapOf("contentType" to "test"))
        }

        val resultStatsAfter= mongoDb.db.runCommand(Document(mapOf("dbstats" to 1, "scale" to 1048576)))
        val fileCollectionStatsAfter= mongoDb.db.runCommand(Document("collStats", "rcs.files"))
        val chunksCollectionStatsAfter= mongoDb.db.runCommand(Document("collStats", "rcs.chunks"))

        logger.info { "**** AFTER ****"}
        //logger.info { "DB stats: $resultStatsAfter" }
        logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStatsAfter["fsUsedSize"] as Int}" }
        //logger.info { "File collection stats: $fileCollectionStatsAfter" }
        //logger.info { "Chunks collection stats: $chunksCollectionStatsAfter" }
        logger.info { "The total size of all indexes"}
        logger.info { "File collection totalIndexSize: ${fileCollectionStatsAfter["totalIndexSize"]}"}
        logger.info { "Chunks collection totalIndexSize: ${chunksCollectionStatsAfter["totalIndexSize"]}"}

        logger.info { "**** DIFFERENCE ****"}
        logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStatsAfter["fsUsedSize"] as Int - resultStats["fsUsedSize"] as Int}" }
        logger.info { "The total uncompressed size in memory of all records in a collection. The size does not include the size of any indexes associated with the collection, which the totalIndexSize field reports."}
        logger.info { "File collection size: ${fileCollectionStatsAfter["size"] as Int - fileCollectionStats["size"] as Int}" }
        logger.info { "Chunks collection size: ${chunksCollectionStatsAfter["size"] as Int - chunksCollectionStats["size"] as Int}" }
        logger.info { "The total size of all indexes"}
        logger.info { "File collection totalIndexSize: ${fileCollectionStatsAfter["totalIndexSize"] as Int - fileCollectionStats["totalIndexSize"] as Int}" }
        logger.info { "Chunks collection totalIndexSize: ${chunksCollectionStatsAfter["totalIndexSize"] as Int - chunksCollectionStats["totalIndexSize"] as Int}" }

    }*/


}