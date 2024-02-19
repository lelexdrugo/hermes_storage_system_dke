package it.polito.test.storage_service.integration.service.chunkService

import it.polito.test.storage_service.repositories.ChunkRepository
import it.polito.test.storage_service.repositories.FileRepository
import it.polito.test.storage_service.services.ChunkService
import it.polito.test.storage_service.services.FileService
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assumptions
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
import kotlin.math.ceil

@SpringBootTest
@Testcontainers
class AppendData {
    companion object {
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

    }

    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var fileRepo: FileRepository

    @Autowired
    private lateinit var chunkRepo: ChunkRepository

    @Autowired
    private lateinit var fileService: FileService

    @Autowired
    private lateinit var chunkService: ChunkService

    @Autowired
    lateinit var gridFs: GridFsTemplate

    @Autowired
    lateinit var mongoDB: MongoTemplate

    val CHUNK_SIZE = 261120

    val CONTENT_SIZE = 10 * CHUNK_SIZE

    //Don't change it. Test are meaningful with this value
    val INITIAL_FILE_SIZE = 0
    //Generate before to speedUp
    val contentByteArray = ByteArray(CONTENT_SIZE).also { Random().nextBytes(it) }

    val filename = "testFile"
    val contentType = "text/plain"
    val defaultOffset = -1L

    //Each test start with an empty existing file
    @BeforeEach
    fun setUp() {
        fileRepo.deleteAll()
        chunkRepo.deleteAll()
        //Create a file with half of the chunkSize byte and add to the rep

        fileService.createFile(filename, byteArrayOf().inputStream(), contentType, defaultOffset, INITIAL_FILE_SIZE.toLong())
        val file = fileRepo.findByFilename(filename)
        logger.info { "File created: $file" }
        //log number of chunks per file
        val chunks = chunkRepo.countAllByFileId(file._id)
        logger.info { "Number of chunks: $chunks" }
        Assertions.assertEquals(file._id, fileRepo.findByFilename(filename)._id) {
            "The file created and the file retrieved from the repo are different"
        }
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), file.length) {
            "File length is ${fileRepo.findByFilename(filename).length} and not $INITIAL_FILE_SIZE"
        }
    }

    @AfterEach
    fun tearDownDb() {
/*        fileRepo.deleteAll()
        chunkRepo.deleteAll()*/
    }

    @Test
    fun `Append bytes to an empty (existing) file without exceeding`() {
        val file = fileRepo.findByFilename(filename)
        //I want that in the beforeeach, the file is created as empty. Just for the semantic of these tests
        Assumptions.assumeTrue(file.length == 0L) { "The file is not empty" }
        val numberOfChunkBefore = chunkRepo.countAllByFileId(file._id)
        Assumptions.assumeTrue(numberOfChunkBefore.toInt() == 0) { "The file already has chunks" }
        //Now begin the logic of the test
        //Generate before to speedUP
        //val bytesToAppend = ByteArray(CHUNK_SIZE/2).also { Random().nextBytes(it) }
        val bytesToAppend = contentByteArray.copyOfRange(0, CHUNK_SIZE / 2)
        chunkService.appendData(file, file._id, bytesToAppend.inputStream(), bytesToAppend.size.toLong())
        val chunkList = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkList.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+1, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not 1"
        }
        val actualLength = chunkList.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend.size.toLong(), actualLength) {
            "The new size of all chunks is $actualLength and not ${file.length + bytesToAppend.size}"
        }
        val lastChunk = chunkList.flatMap { it.data.toList() }.toByteArray()
        Assertions.assertArrayEquals(bytesToAppend, lastChunk) {
            "The last chunk content is $lastChunk and not $bytesToAppend"
        }
    }

    @Test
    fun `Append bytes to an half full chunk without exceeding (append twice)`() {
        var file = fileRepo.findByFilename(filename)

        val bytesToAppend1 = contentByteArray.copyOfRange(0, CHUNK_SIZE / 2)
        chunkService.appendData(file, file._id, bytesToAppend1.inputStream(), bytesToAppend1.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend1.size.toLong())
        val numberOfChunkBefore = chunkRepo.countAllByFileId(file._id)

        file = fileRepo.findByFilename(filename)
        val bytesToAppend2 = contentByteArray.copyOfRange(CHUNK_SIZE / 2, CHUNK_SIZE)
        chunkService.appendData(file, file._id, bytesToAppend2.inputStream(), bytesToAppend2.size.toLong())
        val chunkList = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkList.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not $numberOfChunkBefore"
        }
        val actualLength = chunkList.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend1.size.toLong() + bytesToAppend2.size.toLong(), actualLength) {
            "The new size of all chunks is $actualLength and not ${file.length + bytesToAppend1.size + bytesToAppend2.size}"
        }
        val lastChunk = chunkList.flatMap { it.data.toList() }.toByteArray()
        Assertions.assertArrayEquals(bytesToAppend1 + bytesToAppend2, lastChunk) {
            "The last chunk content is $lastChunk and not ${bytesToAppend1 + bytesToAppend2}"
        }
    }

    @Test
    fun `Append bytes to an empty chunk exceeding the chunk size`() {
        val file = fileRepo.findByFilename(filename)
        val numberOfChunkBefore = chunkRepo.countAllByFileId(file._id)

        val bytesToAppend = contentByteArray.copyOfRange(0, CHUNK_SIZE + 1)
        chunkService.appendData(file, file._id, bytesToAppend.inputStream(), bytesToAppend.size.toLong())
        val chunkList = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkList.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+2, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not 2"
        }
        val actualLength = chunkList.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend.size.toLong(), actualLength) {
            "The new size of all chunks is $actualLength and not ${file.length + bytesToAppend.size}"
        }
        val lastChunk = chunkList.last()
        val expectedBytesInTheLastChunk = bytesToAppend.copyOfRange(CHUNK_SIZE, bytesToAppend.size)
        Assertions.assertArrayEquals(expectedBytesInTheLastChunk, lastChunk.data) {
            "The last chunk content is ${lastChunk.data} and not $expectedBytesInTheLastChunk"
        }
    }

    @Test
    fun `Append bytes to an half full chunk exceeding the chunk size`() {
        var file = fileRepo.findByFilename(filename)

        val bytesToAppend1 = contentByteArray.copyOfRange(0, CHUNK_SIZE / 2)
        chunkService.appendData(file, file._id, bytesToAppend1.inputStream(), bytesToAppend1.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend1.size.toLong())

        val numberOfChunkBeforeExceeding = chunkRepo.countAllByFileId(file._id)

        file = fileRepo.findByFilename(filename)
        val bytesToAppend2 = contentByteArray.copyOfRange(CHUNK_SIZE / 2, CHUNK_SIZE + 1)
        chunkService.appendData(file, file._id, bytesToAppend2.inputStream(), bytesToAppend2.size.toLong())
        val chunkList = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkList.size.toLong()
        Assertions.assertEquals(numberOfChunkBeforeExceeding+1, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not ${numberOfChunkBeforeExceeding+1}"
        }
        val actualLength = chunkList.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend1.size.toLong() + bytesToAppend2.size.toLong(), actualLength) {
            "The new size of all chunks is $actualLength and not ${file.length + bytesToAppend1.size + bytesToAppend2.size}"
        }
        val lastChunk = chunkList.last()
        val lengthBeforeExceeding = bytesToAppend1.size
        val expectedBytesInTheLastChunk = bytesToAppend2.copyOfRange(CHUNK_SIZE-lengthBeforeExceeding, bytesToAppend2.size)
        Assertions.assertArrayEquals(expectedBytesInTheLastChunk, lastChunk.data) {
            "The last chunk content is ${lastChunk.data} and not $expectedBytesInTheLastChunk"
        }
    }

    @Test
    fun `Append multiple times to a chunk`() {
        var file = fileRepo.findByFilename(filename)
        val bytesToAppend1 = contentByteArray.copyOfRange(0, CHUNK_SIZE / 2)
        chunkService.appendData(file, file._id, bytesToAppend1.inputStream(), bytesToAppend1.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend1.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend2 = contentByteArray.copyOfRange(CHUNK_SIZE / 2, CHUNK_SIZE + 1)
        chunkService.appendData(file, file._id, bytesToAppend2.inputStream(), bytesToAppend2.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend2.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend3 = contentByteArray.copyOfRange(CHUNK_SIZE + 1, CHUNK_SIZE * 2)
        chunkService.appendData(file, file._id, bytesToAppend3.inputStream(), bytesToAppend3.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend3.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend4 = contentByteArray.copyOfRange(CHUNK_SIZE * 2, CHUNK_SIZE * 2 + 1)
        chunkService.appendData(file, file._id, bytesToAppend4.inputStream(), bytesToAppend4.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend4.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend5 = contentByteArray.copyOfRange(CHUNK_SIZE * 2 + 1, CHUNK_SIZE * 3)
        chunkService.appendData(file, file._id, bytesToAppend5.inputStream(), bytesToAppend5.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend5.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend6 = contentByteArray.copyOfRange(CHUNK_SIZE * 3, CHUNK_SIZE * 3 + 1)
        chunkService.appendData(file, file._id, bytesToAppend6.inputStream(), bytesToAppend6.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend6.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend7 = contentByteArray.copyOfRange(CHUNK_SIZE * 3 + 1, (CHUNK_SIZE * 3) + (CHUNK_SIZE / 2) + 1)
        chunkService.appendData(file, file._id, bytesToAppend7.inputStream(), bytesToAppend7.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend7.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend8 = contentByteArray.copyOfRange((CHUNK_SIZE * 3) + (CHUNK_SIZE / 2) + 1, CHUNK_SIZE * 4 + 1)
        chunkService.appendData(file, file._id, bytesToAppend8.inputStream(), bytesToAppend8.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend8.size.toLong())

        val totalBytesAppended = contentByteArray.copyOfRange(0, CHUNK_SIZE * 4 + 1)

        Assertions.assertArrayEquals(totalBytesAppended, bytesToAppend1 + bytesToAppend2 + bytesToAppend3 + bytesToAppend4 + bytesToAppend5 + bytesToAppend6 + bytesToAppend7 + bytesToAppend8) {
            "The appended content is ${bytesToAppend1 + bytesToAppend2 + bytesToAppend3 + bytesToAppend4 + bytesToAppend5 + bytesToAppend6 + bytesToAppend7 + bytesToAppend8} and not $totalBytesAppended. Error specifying range"
        }

        val chunkList = chunkRepo.findAllByFileId(file._id)
        val actualLength = chunkList.flatMap { it.data.toList() }.size.toLong()

        //Length is checked also in assertArrayEquals
        Assertions.assertEquals(file.length + totalBytesAppended.size.toLong(), actualLength) {
            "The new size of all chunks is $actualLength and not ${file.length + totalBytesAppended.size}"
        }

        val lastChunk = chunkList.last()
        val expectedBytesInTheLastChunk = totalBytesAppended.copyOfRange(CHUNK_SIZE * 4, CHUNK_SIZE * 4 + 1)
        Assertions.assertArrayEquals(expectedBytesInTheLastChunk, lastChunk.data) {
            "The last chunk content is ${lastChunk.data} and not $expectedBytesInTheLastChunk"
        }

        val actualBytesAppended = chunkList.flatMap { it.data.toList() }.toByteArray()
        Assertions.assertArrayEquals(totalBytesAppended, actualBytesAppended) {
            "The bytes appended are $actualBytesAppended and not $totalBytesAppended"
        }

        val numberOfChunk = chunkList.size.toLong()

        val expectedNumberOfChunks = ceil(totalBytesAppended.size.toDouble() / CHUNK_SIZE.toDouble()).toLong()
        Assertions.assertEquals(expectedNumberOfChunks, numberOfChunk) {
            "The number of chunks is $numberOfChunk and not $expectedNumberOfChunks"
        }
    }

    @Test
    fun `Append multiple times to a chunk and compare with a gridfs created file`() {
        var file = fileRepo.findByFilename(filename)
        val bytesToAppend1 = contentByteArray.copyOfRange(0, CHUNK_SIZE / 2)
        chunkService.appendData(file, file._id, bytesToAppend1.inputStream(), bytesToAppend1.size.toLong())
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend1.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend2 = contentByteArray.copyOfRange(CHUNK_SIZE / 2, CHUNK_SIZE + 1)
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend2.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend3 = contentByteArray.copyOfRange(CHUNK_SIZE + 1, CHUNK_SIZE * 2)
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend3.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend4 = contentByteArray.copyOfRange(CHUNK_SIZE * 2, CHUNK_SIZE * 2 + 1)
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend4.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend5 = contentByteArray.copyOfRange(CHUNK_SIZE * 2 + 1, CHUNK_SIZE * 3)
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend5.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend6 = contentByteArray.copyOfRange(CHUNK_SIZE * 3, CHUNK_SIZE * 3 + 1)
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend6.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend7 = contentByteArray.copyOfRange(CHUNK_SIZE * 3 + 1, (CHUNK_SIZE * 3) + (CHUNK_SIZE / 2) + 1)
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend7.size.toLong())

        file = fileRepo.findByFilename(filename)
        val bytesToAppend8 = contentByteArray.copyOfRange((CHUNK_SIZE * 3) + (CHUNK_SIZE / 2) + 1, CHUNK_SIZE * 4 + 1)
        fileRepo.findAndIncrementLengthBy_id(file._id, bytesToAppend8.size.toLong())

        val totalBytesAppended = contentByteArray.copyOfRange(0, CHUNK_SIZE * 4 + 1)

        Assertions.assertArrayEquals(totalBytesAppended, bytesToAppend1 + bytesToAppend2 + bytesToAppend3 + bytesToAppend4 + bytesToAppend5 + bytesToAppend6 + bytesToAppend7 + bytesToAppend8) {
            "The appended content is ${bytesToAppend1 + bytesToAppend2 + bytesToAppend3 + bytesToAppend4 + bytesToAppend5 + bytesToAppend6 + bytesToAppend7 + bytesToAppend8} and not $totalBytesAppended"
        }

        val chunkListOfAppendedFile = chunkRepo.findAllByFileId(file._id)
        val actualBytesAppendedOfAppendedFile = chunkListOfAppendedFile.flatMap { it.data.toList() }.toByteArray()
        //I've tested this situation in the test before, but redo it for the sake of completeness
        Assertions.assertArrayEquals(totalBytesAppended, actualBytesAppendedOfAppendedFile) {
            "The bytes appended are $actualBytesAppendedOfAppendedFile and not $totalBytesAppended"
        }

        val filenameOfFullStoredFile = "completeFileDuringCreation"
        val fileFullStoredId = gridFs.store(totalBytesAppended.inputStream(), filenameOfFullStoredFile, contentType)

        val chunkListOfFullStoredFile = chunkRepo.findAllByFileId(fileFullStoredId)
        val actualBytesAppendedOfFullStoredFile = chunkListOfFullStoredFile.flatMap { it.data.toList() }.toByteArray()

        Assertions.assertArrayEquals(actualBytesAppendedOfFullStoredFile, actualBytesAppendedOfAppendedFile) {
            "The bytes appended are $actualBytesAppendedOfAppendedFile and not $actualBytesAppendedOfFullStoredFile"
        }

        val numberOfChunkOfAppendedFile = chunkListOfAppendedFile.size.toLong()
        val numberOfChunkOfFullStoredFile = chunkListOfFullStoredFile.size.toLong()
        Assertions.assertEquals(numberOfChunkOfFullStoredFile, numberOfChunkOfAppendedFile) {
            "The number of chunks is $numberOfChunkOfAppendedFile and not $numberOfChunkOfFullStoredFile"
        }
    }
}