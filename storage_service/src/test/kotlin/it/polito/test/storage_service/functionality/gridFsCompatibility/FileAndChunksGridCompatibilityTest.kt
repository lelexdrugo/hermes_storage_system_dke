package it.polito.test.storage_service.functionality.gridFsCompatibility

import it.polito.test.storage_service.documents.Chunk
import it.polito.test.storage_service.repositories.ChunkRepository
import it.polito.test.storage_service.repositories.FileRepository
import it.polito.test.storage_service.services.ChunkService
import it.polito.test.storage_service.services.FileService
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ResourceLoader
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.io.path.Path


@SpringBootTest
@Testcontainers
//@ActiveProfiles("logging-test")
class FileAndChunksGridCompatibilityTest {
    companion object {
        @Container
        var container: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.mongodb.uri", container::getReplicaSetUrl)
            //registry.add("spring.data.mongodb.username", container::getReplicaSetUrl)
            //registry.add("spring.data.mongodb.password",  )
        }
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

/*    @Autowired
    lateinit var restTemplate: TestRestTemplate*/

    @Autowired
    lateinit var gridFs: GridFsTemplate


    @Autowired
    lateinit var mongoDB: MongoTemplate

    @AfterEach
    fun tearDownDb() {
        fileRepo.deleteAll()
        chunkRepo.deleteAll()
    }

    @Autowired
    private lateinit var resourceLoader: ResourceLoader

    final val CHUNK_SIZE = 261120

    final val CONTENT_SIZE = 10*CHUNK_SIZE
    val contentByteArray = ByteArray(CONTENT_SIZE).also { Random().nextBytes(it) }

    @Test
    fun `create a file with gridFS`() {
        val filename = "testFile"
        val inputUri = resourceLoader.getResource("classpath:pic.jpg").uri
//        val inputStream: InputStream = contentByteArray.inputStream()
        resourceLoader.getResource("classpath:pic.jpg")
        val inputStream = File(inputUri).inputStream()
        val savedId = gridFs.store(inputStream, filename, "image/jpg")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
    }

    @Test
    fun `create and retrieve a file with gridFS`() {
        val filename = "testFile"
        val outputFileName = "retrievedFile.jpg"
        val inputUri = resourceLoader.getResource("classpath:pic.jpg").uri
//        val inputStream: InputStream = contentByteArray.inputStream()
        resourceLoader.getResource("classpath:pic.jpg")
        val inputStream = File(inputUri).inputStream()
        val savedId = gridFs.store(inputStream, filename, "image/jpg")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
        val retrievedResFile = gridFs.getResource(filename)

        val output = FileOutputStream(outputFileName, false)
        //content or inputStream
        retrievedResFile.content.transferTo(output)
        val result = Files.mismatch(Path.of(inputUri), Path(outputFileName))

        Assertions.assertEquals(result, -1, "Content doesn't match")
    }

    @Test
    fun `create a file with gridFS and print all the chunks`() {
        val filename = "testFile"
        val inputUri = resourceLoader.getResource("classpath:pic.jpg").uri
//        val inputStream: InputStream = contentByteArray.inputStream()
        resourceLoader.getResource("classpath:pic.jpg")
        val inputStream = File(inputUri).inputStream()
        val savedId = gridFs.store(inputStream, filename, "image/jpg")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        println(chunkRepo.countAllByFileId(savedId))
        chunkRepo.findAllByFileId(savedId).forEachIndexed { index, chunk ->  println("Chunk number $index\t id:${chunk._id}\t file_id:${chunk.fileId} \t n:${chunk.n}\t data:${chunk.data.first()}...") }
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
    }

    @Test
    fun `create a file chunk by chunk and retrieve it with gridfs - first chunk is a full document`() {
        val filename = "testFile"
        val chunkDimension = CHUNK_SIZE
        val outputFileName = "retrievedFile.bin"
        /*** Create a file with the first CHUNK_SIZE bytes ***/
        val chunk = contentByteArray.take(CHUNK_SIZE).toByteArray().inputStream()
        //val inputStream: InputStream = contentByteArray.inputStream()
        val savedId = gridFs.store(chunk, filename, "data/bin")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        println(chunkRepo.countAllByFileId(savedId))
        chunkRepo.findAllByFileId(savedId).forEachIndexed { index, chunk ->  println("Chunk number $index\t id:${chunk._id}\t file_id:${chunk.fileId} \t n:${chunk.n}\t data:${chunk.data.first()}...") }
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
        var retrievedResFile = gridFs.getResource(filename)
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(retrievedResFile.content.readBytes().size, chunkDimension, "Problem with the bytearray")
        //val output = FileOutputStream(outputFileName, false)
        //retrievedResFile.content.transferTo(output)
        //Assertions.assertEquals(Files.size(Path(outputFileName)), chunkDimension.toLong(), "Size doesn't match")
        /*** Create a new chunk document (I choose to make the first document full), appending manually 200 bytes to the file document ***/
        chunkRepo.save(Chunk(
            fileId = savedId,
            n = 1,
            data = contentByteArray.slice(CHUNK_SIZE..CHUNK_SIZE+199).toByteArray(),
        ))
        retrievedResFile = gridFs.getResource(filename)
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(retrievedResFile.content.readBytes().size, chunkDimension, "Retrieved more byte than the length")
        /*** Update the length ***/
        fileRepo.findAndIncrementLengthBy_id(savedId, 200)

        retrievedResFile = gridFs.getResource(filename)
        /*** Retrieve and check that obtains CHUNK_SIZE+200 bytes ***/
        Assertions.assertEquals(retrievedResFile.content.readBytes().size, chunkDimension+200, "Retrieved less byte than the length")
    }

    /**
     * Ignore this test (otherwise assert that mongoGridFSException is launched) because is the one that prove that there is a problem using the read api of gridfs
     * When manually append data and someone try to read them with getResource by gridFs and the lenght is not yet updated
     * Chunk size data length is not the expected size. The size was 400 for file_id: BsonObjectId{value=638f17af11e0ea5211912397} chunk index 0 it should be 200 bytes.
     * com.mongodb.MongoGridFSException: Chunk size data length is not the expected size. The size was 400 for file_id:
     * https://www.tabnine.com/code/java/methods/com.mongodb.MongoGridFSException/%3Cinit%3E#:~:text=throw%20new%20MongoGridFSException(format(%22Chunk,data.length%2C%20fileId%2C%20expectedChunkIndex%2C%20expectedDataLength))%3B
     */
    @Disabled("This test prove that gridfs doesn't allow reading when the length is not yet updated")
    @Test
    fun `create a file chunk by chunk and retrieve it with gridfs - first chunk is not a full document`() {
        val filename = "testFile"
        val chunkDimension = 200
        val outputFileName = "retrievedFile.bin"
        /*** Create a file with the first 200 bytes ***/
        val chunk = contentByteArray.take(200).toByteArray().inputStream()
        val savedId = gridFs.store(chunk, filename, "data/bin")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
        val retrievedResFile = gridFs.getResource(filename)
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(retrievedResFile.content.readBytes().size, chunkDimension, "Problem with the bytearray")
        /*** Modify last chunk document, appending manually 200 bytes to the file document ***/
        val createdChunk = chunkRepo.findByFileIdAndN(savedId, 0)
        chunkRepo.findAndSetDataBy_id(createdChunk._id, createdChunk.data.plus(contentByteArray.slice(200..399).toByteArray()))
        //chunkRepo.findAndPushDataByFileIdAndN(savedId, 0, contentByteArray.slice(200..399).toByteArray())
/*        chunkRepo.save(Chunk(
            fileId = savedId,
            n = 0,
            data = contentByteArray.slice(200..399).toByteArray()
        ))*/
        val updatedChunk = chunkRepo.findAllByFileId(savedId)
        Assertions.assertEquals(updatedChunk.map { it.data }.sumOf { it.size }, 2*chunkDimension, "Stored less byte")

        /***! Questo metodo rompe gridFS, perché se proviamo a leggere prima di aggiornare la length otteniamo l'errore indicato dopo ***/
        val secondRetrievedResFile = gridFs.getResource(filename)

        /***! Qui il problema! Nel caso di lettura prima della modifica della length GridFS solleva un'eccezione. Anche se il valore retrieveResFile viene valorizzato come vorrei... a eccezione di un flag 'read' settato a false ***/
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(secondRetrievedResFile.content.readBytes().size, chunkDimension, "Retrieved more byte than the length")
        /*** Update the length ***/
        fileRepo.findAndIncrementLengthBy_id(savedId, 200)

        val thirdRetrievedResFile = gridFs.getResource(filename)
        /*** Retrieve and check that obtains CHUNK_SIZE+200 bytes ***/
        Assertions.assertEquals(thirdRetrievedResFile.content.readBytes().size, chunkDimension+200, "Retrieved less byte than the length")
    }

    @Test
    fun `create a file chunk by chunk and retrieve it with gridfs when all the operations are completed - first chunk is not a full document - made with the service method`() {
        val filename = "testFile"
        val chunkDimension = 200
        val outputFileName = "retrievedFile.bin"
        /*** Create a file with the first 200 bytes ***/
        val chunk = contentByteArray.take(200).toByteArray().inputStream()
        val savedId = gridFs.store(chunk, filename, "data/bin")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
        var retrievedResFile = gridFs.getResource(filename)
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(retrievedResFile.content.readBytes().size, chunkDimension, "Problem with the bytearray")
        /*** Modify last chunk document, appending 200 bytes to the file document, and incrementing length to root file document ***/
        /*** Alteratively, we can use the service method addChunkToAFile that wrap the two operation***/
        val file = fileRepo.findByFilename(filename)
        chunkService.appendData(file, savedId, contentByteArray.slice(200..399).toByteArray().inputStream(), 200)
        fileRepo.findAndIncrementLengthBy_id(savedId, 200)

        retrievedResFile = gridFs.getResource(filename)
        /*** Retrieve and check that obtains CHUNK_SIZE+200 bytes ***/
        Assertions.assertEquals(retrievedResFile.content.readBytes().size, chunkDimension+200, "Retrieved less byte than the length")
    }

    @Test
    fun `create a file chunk by chunk and retrieve it with custom service method - first chunk is not a full document`() {
        val filename = "testFile"
        val chunkDimension = 200
        /*** Create a file with the first 200 bytes ***/
        val chunk = contentByteArray.take(200).toByteArray().inputStream()
        val savedId = gridFs.store(chunk, filename, "data/bin")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
        val retrievedResFile = fileService.getEntireResource(filename)
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(retrievedResFile.data.size, chunkDimension, "Problem with the bytearray")
        /*** Modify last chunk document, appending manually 200 bytes to the file document ***/
        val createdChunk = chunkRepo.findByFileIdAndN(savedId, 0)
        chunkRepo.findAndSetDataBy_id(createdChunk._id, createdChunk.data.plus(contentByteArray.slice(200..399).toByteArray()))
        val updatedChunk = chunkRepo.findAllByFileId(savedId)
        logger.trace("Number of chunks for file '$filename': ${updatedChunk.size}")
        logger.trace("Stored by '$filename': ${updatedChunk.map { it.data }.sumOf { it.size }}")
        Assertions.assertEquals(updatedChunk.map { it.data }.sumOf { it.size }, 2*chunkDimension, "Stored less byte")
        val secondRetrievedResFile = fileService.getEntireResource(filename)

        logger.trace("Readable bytes: ${secondRetrievedResFile.data.size}")
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(secondRetrievedResFile.data.size, chunkDimension, "Retrieved more byte than the length")
        /** Update the length **/
        fileRepo.findAndIncrementLengthBy_id(savedId, 200)

        val thirdRetrievedResFile = fileService.getEntireResource(filename)
        /** Retrieve and check that obtains CHUNK_SIZE+200 bytes **/
        logger.trace("Readable bytes after length update: ${thirdRetrievedResFile.data.size}")
        Assertions.assertEquals(thirdRetrievedResFile.data.size, chunkDimension+200, "Retrieved less byte than the length")
    }

    @Test
    fun `create a file chunk by chunk and retrieve it with custom service method when all the operations are completed - first chunk is not a full document`() {
        val filename = "testFile"
        val chunkDimension = 200
        /*** Create a file with the first 200 bytes ***/
        val chunk = contentByteArray.take(200).toByteArray().inputStream()
        val savedId = gridFs.store(chunk, filename, "data/bin")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        Assertions.assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
        var retrievedResFile = fileService.getEntireResource(filename)
        /*** Retrieve and check that obtains only CHUNK_SIZE bytes ***/
        Assertions.assertEquals(retrievedResFile.data.size, chunkDimension, "Problem with the bytearray")
        /*** Modify last chunk document, appending 200 bytes to the file document, and incrementing length to root file document ***/
        /*** Alteratively, we can use the service method addChunkToAFile that wrap the two operation***/
        val file = fileRepo.findByFilename(filename)
        chunkService.appendData(file, savedId, contentByteArray.slice(200..399).toByteArray().inputStream(), 200)
        fileRepo.findAndIncrementLengthBy_id(savedId, 200)

        retrievedResFile = fileService.getEntireResource(filename)
        /*** Retrieve and check that obtains CHUNK_SIZE+200 bytes ***/
        Assertions.assertEquals(retrievedResFile.data.size, chunkDimension+200, "Retrieved less byte than the length")
    }
}