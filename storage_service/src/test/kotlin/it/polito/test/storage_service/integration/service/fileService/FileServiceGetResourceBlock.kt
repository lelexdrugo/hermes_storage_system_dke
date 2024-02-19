package it.polito.test.storage_service.integration.service.fileService

import it.polito.test.storage_service.repositories.ChunkRepository
import it.polito.test.storage_service.repositories.FileRepository
import it.polito.test.storage_service.services.ChunkService
import it.polito.test.storage_service.services.FileService
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.*

//TODO reintroduce the testcontainers (using embedded to test quickly)
//To use embedded no running instance
//@ActiveProfiles("logging-test")
//@DataMongoTest
@SpringBootTest
@Testcontainers
class FileServiceGetResourceBlock {
    companion object{
        @Container
        var container: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
        //val mongoContainer = GenericContainer("mongo:4.4.2")
/*            .withExposedPorts(27017)
            .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
            .withEnv("MONGO_INITDB_ROOT_PASSWORD", "root")
            .withEnv("MONGO_INITDB_DATABASE", "test")*/

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.mongodb.uri", container::getReplicaSetUrl)
            logger.info { "MongoDBContainer: ${container.replicaSetUrl}" }

        }
        private val logger = KotlinLogging.logger {}
    }

    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var fileRepo: FileRepository

    @Autowired
    private lateinit var chunkRepo: ChunkRepository

    @Autowired
    private lateinit var fileService : FileService

    @Autowired
    private lateinit var chunkService : ChunkService

    @Autowired
    lateinit var gridFs: GridFsTemplate

    @Autowired
    lateinit var mongoDB: MongoTemplate


    val CHUNK_SIZE = 261120

    val CONTENT_SIZE = 10*CHUNK_SIZE
    //val INITIAL_FILE_SIZE = CHUNK_SIZE/2
    val INITIAL_FILE_SIZE =CONTENT_SIZE
    val contentByteArray = ByteArray(CONTENT_SIZE).also { Random().nextBytes(it) }

    @BeforeEach
    fun setUp() {
        fileRepo.deleteAll()
        chunkRepo.deleteAll()
        //Create a file with half of the chunkSize byte and add to the repo
        val filename = "testFile"
        val contentType = "text/plain"
        val defaultOffset = -1L
        fileService.createFile(filename, contentByteArray.take(INITIAL_FILE_SIZE).toByteArray().inputStream(), contentType, defaultOffset, INITIAL_FILE_SIZE.toLong())
        val file = fileRepo.findByFilename(filename)
            logger.info { "File created: $file" }
        assertEquals(file._id, fileRepo.findByFilename(filename)._id){
            "The file created and the file retrieved from the repo are different"
        }
        assertEquals(INITIAL_FILE_SIZE.toLong(), file.length) {
            "File length is ${fileRepo.findByFilename(filename).length} and not $INITIAL_FILE_SIZE"
        }
    }

    @AfterEach
    fun tearDownDb() {
/*        fileRepo.deleteAll()
        chunkRepo.deleteAll()*/
    }

    @Test
    fun `Get a block of a file`() {
        val filename = "testFile"
        val blockSize = 1000
        val offset = 0
        val fileResourceBlock = fileService.getResourceBlock(filename, blockSize, offset)
        assertEquals(filename, fileResourceBlock.file.filename, "The filename is not the same")
 //       assertEquals(fileResourceBlock.file.contentType, "text/plain")
        assertEquals(blockSize, fileResourceBlock.requestedBlockSize, "The requested block size is not the same")
        assertEquals(offset, fileResourceBlock.offset, "The offset is not the same")
        assertEquals(blockSize, fileResourceBlock.data.size, "The data size is not the same")
        assertEquals(fileResourceBlock.returnedBlockSize, fileResourceBlock.data.size, "The returned block size is not the same")
        assertTrue(contentByteArray.take(blockSize).toByteArray().contentEquals(fileResourceBlock.data), "The data is not the same")
    }

    @Test
    fun `Get a block of a file with offset`() {
        val filename = "testFile"
        val blockSize = 1000
        val offset = 1000
        val fileResourceBlock = fileService.getResourceBlock(filename, blockSize, offset)
        assertEquals(filename, fileResourceBlock.file.filename)
        assertEquals(fileResourceBlock.file.contentType, "text/plain")
        assertEquals(blockSize, fileResourceBlock.requestedBlockSize)
        assertEquals(offset, fileResourceBlock.offset)
        assertEquals(blockSize, fileResourceBlock.data.size)
        assertEquals(fileResourceBlock.returnedBlockSize, fileResourceBlock.data.size)
        assertTrue(contentByteArray.sliceArray(offset until offset+blockSize).contentEquals(fileResourceBlock.data))
    }

    @Test
    fun `Get a block of a file with offset and block size greater than the file size`() {
        val filename = "testFile"
        val blockSize = 2*INITIAL_FILE_SIZE
        val offset = 1000
        val fileResourceBlock = fileService.getResourceBlock(filename, blockSize, offset)
        assertEquals(filename, fileResourceBlock.file.filename, "The filename is not the same")
        assertEquals(fileResourceBlock.file.contentType, "text/plain")
        assertEquals(blockSize, fileResourceBlock.requestedBlockSize, "The requested block size is not the same")
        assertEquals(offset, fileResourceBlock.offset, "The offset is not the same")
        assertEquals(INITIAL_FILE_SIZE-offset, fileResourceBlock.data.size, "The data size is not the same")
        assertEquals(fileResourceBlock.returnedBlockSize, fileResourceBlock.data.size, "The returned block size is not the same")
        assertTrue(contentByteArray.sliceArray(offset until INITIAL_FILE_SIZE).contentEquals(fileResourceBlock.data), "The data is not the same")
    }


    @Test
    fun `Get a block of a file with offset greater than the file size`() {
        val filename = "testFile"
        val blockSize = 1000
        val offset = 2*INITIAL_FILE_SIZE
        val fileResourceBlock = fileService.getResourceBlock(filename, blockSize, offset)
        assertEquals(filename, fileResourceBlock.file.filename)
        assertEquals(fileResourceBlock.file.contentType, "text/plain")
        assertEquals(blockSize, fileResourceBlock.requestedBlockSize)
        assertEquals(offset, fileResourceBlock.offset)
        assertEquals(0, fileResourceBlock.data.size)
        assertEquals(fileResourceBlock.returnedBlockSize, fileResourceBlock.data.size)
    }

    @Test
    fun `Get a block of a file with offset greater than the file size and block size greater than the file size`() {
        val filename = "testFile"
        val blockSize = 2*INITIAL_FILE_SIZE
        val offset = 2*INITIAL_FILE_SIZE
        val fileResourceBlock = fileService.getResourceBlock(filename, blockSize, offset)
        assertEquals(filename, fileResourceBlock.file.filename)
        assertEquals(fileResourceBlock.file.contentType, "text/plain")
        assertEquals(blockSize, fileResourceBlock.requestedBlockSize)
        assertEquals(offset, fileResourceBlock.offset)
        assertEquals(0, fileResourceBlock.data.size)
        assertEquals(fileResourceBlock.returnedBlockSize, fileResourceBlock.data.size)
    }

    @Test
    fun `Get a default block with offset and without block size`() {
        val filename = "testFile"
        val offset = 1000
        val fileResourceBlock = fileService.getResourceBlock(filename, null, offset)
        assertEquals(filename, fileResourceBlock.file.filename, "The filename is not the same")
        assertEquals(fileResourceBlock.file.contentType, "text/plain")
        assertEquals(0, fileResourceBlock.requestedBlockSize, "The requested block size is not the same")
        assertEquals(offset, fileResourceBlock.offset, "The offset is not the same")
        assertEquals(INITIAL_FILE_SIZE-offset, fileResourceBlock.data.size, "The data size is not the same")
        assertEquals(fileResourceBlock.returnedBlockSize, fileResourceBlock.data.size, "The returned block size is not the same")
        //For the moment the default block size is the chunk size
        assertTrue(contentByteArray.sliceArray(offset until  INITIAL_FILE_SIZE).contentEquals(fileResourceBlock.data), "The data is not the same")
    }

    @Test
    fun `Get a default block with block size and without offset`() {
        val filename = "testFile"
        val blockSize = 1000
        val fileResourceBlock = fileService.getResourceBlock(filename, blockSize, null)
        assertEquals(filename, fileResourceBlock.file.filename)
        assertEquals(fileResourceBlock.file.contentType, "text/plain")
        assertEquals(blockSize, fileResourceBlock.requestedBlockSize)
        assertEquals(0, fileResourceBlock.offset)
        assertEquals(blockSize, fileResourceBlock.data.size)
        assertEquals(fileResourceBlock.returnedBlockSize, fileResourceBlock.data.size)
        assertTrue(contentByteArray.take(blockSize).toByteArray().contentEquals(fileResourceBlock.data))
    }

}