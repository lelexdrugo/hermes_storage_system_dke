package it.polito.test.storage_service.integration.service.fileService

import it.polito.test.storage_service.repositories.ChunkRepository
import it.polito.test.storage_service.repositories.FileRepository
import it.polito.test.storage_service.services.ChunkService
import it.polito.test.storage_service.services.FileService
import mu.KotlinLogging
import org.junit.jupiter.api.*
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

@SpringBootTest
@Testcontainers
class AppendDataWithInterruption {
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

    //Test if the file is correctly updated after an interruption
    //I don't use the Kafka topic, but simulate manually the process of consuming a message with consumerManageFileStorage

    //Specifically, the updateFile function.
    //When a file is created or updated, in the end fileService.updateFile is called.
    //This function call chunkService.updateChunksOfAFile and then update length and lastOffset of the file
    //Calling the updateChunksOfAFile more than one time, simulating the consuming of the same message more than one time, without updating file document, should test idempotence

    @Test
    fun `Interrupt update of an existing empty file (no newer chunk document created)`(){
        val file = fileRepo.findByFilename(filename)
        //I want that in the beforeeach, the file is created as empty. Just for the semantic of these tests
        Assumptions.assumeTrue(file.length == 0L) { "The file is not empty" }
        val numberOfChunkBefore = chunkRepo.countAllByFileId(file._id)
        Assumptions.assumeTrue(numberOfChunkBefore.toInt() == 0) { "The file already has chunks" }
        //Now begin the logic of the test
        val bytesToAppend = contentByteArray.copyOfRange(0, CHUNK_SIZE/2)
        // This lead to the creation of 3 full document in the collection of chunks
        chunkService.appendData(file, file._id, bytesToAppend.inputStream(), bytesToAppend.size.toLong())
        val chunkList = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkList.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+1, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not 1"
        }
        val afterInterruptionLength = chunkList.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend.size.toLong(), afterInterruptionLength) {
            "The new size of all chunks is $afterInterruptionLength and not ${file.length + bytesToAppend.size.toLong()}"
        }
        val fileAfter = fileRepo.findByFilename(filename)
        Assertions.assertEquals(file.length, fileAfter.length) {
            "The size of the file is ${fileAfter.length} and not ${file.length}. Length is change."
        }
        Assertions.assertEquals(file.lastOffset, fileAfter.lastOffset) {
            "The lastOffset of the file is ${fileAfter.lastOffset} and not ${file.lastOffset}. LastOffset is change."
        }
        val fileAfterRetrieved = fileService.getEntireResource(filename)
        Assertions.assertArrayEquals(byteArrayOf(), fileAfterRetrieved.data) {
            "The content of the file retrieved is different from the initial (empty) content."
        }
    }

    @Test
    fun `Interrupt update of an existing empty file (chunk documents created)`(){
        val file = fileRepo.findByFilename(filename)
        //I want that in the beforeeach, the file is created as empty. Just for the semantic of these tests
        Assumptions.assumeTrue(file.length == 0L) { "The file is not empty" }
        val numberOfChunkBefore = chunkRepo.countAllByFileId(file._id)
        Assumptions.assumeTrue(numberOfChunkBefore.toInt() == 0) { "The file already has chunks" }
        //Now begin the logic of the test
        val bytesToAppend = contentByteArray.copyOfRange(0, 3*CHUNK_SIZE)
        // This lead to the creation of 3 full document in the collection of chunks
        chunkService.appendData(file, file._id, bytesToAppend.inputStream(), bytesToAppend.size.toLong())
        val chunkList = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkList.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+3, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not 3"
        }
        val afterInterruptionLength = chunkList.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend.size.toLong(), afterInterruptionLength) {
            "The new size of all chunks is $afterInterruptionLength and not ${file.length + bytesToAppend.size.toLong()}"
        }
        val fileAfter = fileRepo.findByFilename(filename)
        Assertions.assertEquals(file.length, fileAfter.length) {
            "The size of the file is ${fileAfter.length} and not ${file.length}. Length is change."
        }
        Assertions.assertEquals(file.lastOffset, fileAfter.lastOffset) {
            "The lastOffset of the file is ${fileAfter.lastOffset} and not ${file.lastOffset}. LastOffset is change."
        }
        val fileAfterRetrieved = fileService.getEntireResource(filename)
        Assertions.assertArrayEquals(byteArrayOf(), fileAfterRetrieved.data) {
            "The content of the file retrieved is different from the initial (empty) content."
        }
    }

    @Test
    fun `Idempotent update of an existing empty file (all the chunks created when interrupted so skipped update)`() {
        var file = fileRepo.findByFilename(filename)
        //I want that in the beforeeach, the file is created as empty. Just for the semantic of these tests
        Assumptions.assumeTrue(file.length == 0L) { "The file is not empty" }
        val numberOfChunkBefore = chunkRepo.countAllByFileId(file._id)
        Assumptions.assumeTrue(numberOfChunkBefore.toInt() == 0) { "The file already has chunks" }
        //Now begin the logic of the test
        val bytesToAppend = contentByteArray.copyOfRange(0, 3*CHUNK_SIZE)
        /** First message consuming **/
        // This lead to the creation of 3 full document in the collection of chunks
        chunkService.appendData(file, file._id, bytesToAppend.inputStream(), bytesToAppend.size.toLong())
        val chunkListAfterFirstMessage = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkListAfterFirstMessage.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+3, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not 3"
        }
        val afterInterruptionLength = chunkListAfterFirstMessage.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend.size.toLong(), afterInterruptionLength) {
            "The new size of all chunks is $afterInterruptionLength and not ${file.length + bytesToAppend.size.toLong()}"
        }
        val fileAfter = fileRepo.findByFilename(filename)
        Assertions.assertEquals(file.length, fileAfter.length) {
            "The size of the file is ${fileAfter.length} and not ${file.length}. Length is change."
        }
        Assertions.assertEquals(file.lastOffset, fileAfter.lastOffset) {
            "The lastOffset of the file is ${fileAfter.lastOffset} and not ${file.lastOffset}. LastOffset is change."
        }
        val fileAfterRetrieved = fileService.getEntireResource(filename)
        Assertions.assertArrayEquals(byteArrayOf(), fileAfterRetrieved.data) {
            "The content of the file retrieved is different from the initial (empty) content."
        }
        /** Second message consuming, test idempotence **/
        file = fileRepo.findByFilename(filename)
        chunkService.appendData(file, file._id, bytesToAppend.inputStream(), bytesToAppend.size.toLong())
        val chunkListAfterSecondMessage = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfterSecondMessage = chunkListAfterSecondMessage.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+3, numberOfChunkAfterSecondMessage) {
            "The number of chunks is $numberOfChunkAfterSecondMessage and not 3"
        }
        /** No death chunks are left. **/
        Assertions.assertEquals(numberOfChunkAfter, numberOfChunkAfterSecondMessage) {
            "The number of chunks is $numberOfChunkAfterSecondMessage and not 3"
        }

        /** First updating failed after creating all the chunks contained in the message. So the second update doesn't update or create other document in chunk collection. **/
        val chunkListAfterFirstMessageId = chunkListAfterFirstMessage.map { it._id }
        val chunkListAfterSecondMessageId = chunkListAfterSecondMessage.map { it._id }
        Assertions.assertIterableEquals(chunkListAfterFirstMessageId, chunkListAfterSecondMessageId) {
            "The list of chunks id is different"
        }
        val afterInterruptionLengthSecondMessage = chunkListAfterSecondMessage.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppend.size.toLong(), afterInterruptionLengthSecondMessage) {
            "The new size of all chunks is $afterInterruptionLengthSecondMessage and not ${file.length + bytesToAppend.size.toLong()}"
        }
        val fileAfterSecondMessage = fileRepo.findByFilename(filename)
        Assertions.assertEquals(file.length, fileAfterSecondMessage.length) {
            "The size of the file is ${fileAfterSecondMessage.length} and not ${file.length}. Length is change."
        }
        Assertions.assertEquals(file.lastOffset, fileAfterSecondMessage.lastOffset) {
            "The lastOffset of the file is ${fileAfterSecondMessage.lastOffset} and not ${file.lastOffset}. LastOffset is change."
        }
        val fileAfterRetrievedSecondMessage = fileService.getEntireResource(filename)
        Assertions.assertArrayEquals(byteArrayOf(), fileAfterRetrievedSecondMessage.data) {
            "The content of the file retrieved is different from the initial (empty) content."
        }
        /** Updated not interrupted, document in file collection is updated **/
        fileRepo.findAndSetLastOffsetAndIncrementLengthBy_id(file._id, file.lastOffset+1, bytesToAppend.size.toLong())
        val fileAfterRetrievedUpdated = fileService.getEntireResource(filename)

        Assertions.assertEquals(file.length + bytesToAppend.size.toLong(), fileAfterRetrievedUpdated.file.length) {
            "The size of the file is ${fileAfterRetrievedUpdated.file.length} and not ${file.length + bytesToAppend.size.toLong()}."
        }
        Assertions.assertEquals(file.lastOffset + 1 , fileAfterRetrievedUpdated.file.lastOffset) {
            "The lastOffset of the file is ${fileAfterRetrievedUpdated.file.lastOffset} and not ${file.lastOffset + 1}."
        }
        Assertions.assertArrayEquals(bytesToAppend, fileAfterRetrievedUpdated.data) {
            "The content of the file retrieved is different from the updated one expected."
        }
    }

    @Test
    fun `Idempotent update of an existing empty file (some chunks created when interrupted so completed update)`(){
        var file = fileRepo.findByFilename(filename)
        //I want that in the beforeeach, the file is created as empty. Just for the semantic of these tests
        Assumptions.assumeTrue(file.length == 0L) { "The file is not empty" }
        val numberOfChunkBefore = chunkRepo.countAllByFileId(file._id)
        Assumptions.assumeTrue(numberOfChunkBefore.toInt() == 0) { "The file already has chunks" }
        //Now begin the logic of the test
        val bytesToAppendMessage = contentByteArray.copyOfRange(0, 3*CHUNK_SIZE)
        /** First message consuming **/
        val bytesAppendedBeforeInterruption = bytesToAppendMessage.copyOfRange(0, 2*CHUNK_SIZE)
        // This lead to the creation of 2 full document in the collection of chunks
        chunkService.appendData(file, file._id, bytesAppendedBeforeInterruption.inputStream(), bytesAppendedBeforeInterruption.size.toLong())
        val chunkListAfterFirstMessage = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfter = chunkListAfterFirstMessage.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+2, numberOfChunkAfter) {
            "The number of chunks is $numberOfChunkAfter and not 2"
        }
        val afterInterruptionLength = chunkListAfterFirstMessage.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesAppendedBeforeInterruption.size.toLong(), afterInterruptionLength) {
            "The new size of all chunks is $afterInterruptionLength and not ${file.length + bytesAppendedBeforeInterruption.size.toLong()}"
        }
        val fileAfter = fileRepo.findByFilename(filename)
        Assertions.assertEquals(file.length, fileAfter.length) {
            "The size of the file is ${fileAfter.length} and not ${file.length}. Length is change."
        }
        Assertions.assertEquals(file.lastOffset, fileAfter.lastOffset) {
            "The lastOffset of the file is ${fileAfter.lastOffset} and not ${file.lastOffset}. LastOffset is change."
        }
        val fileAfterRetrieved = fileService.getEntireResource(filename)
        Assertions.assertArrayEquals(byteArrayOf(), fileAfterRetrieved.data) {
            "The content of the file retrieved is different from the initial (empty) content."
        }
        /** Second message consuming, test idempotence **/
        file = fileRepo.findByFilename(filename)
        chunkService.appendData(file, file._id, bytesToAppendMessage.inputStream(), bytesToAppendMessage.size.toLong())
        val chunkListAfterSecondMessage = chunkRepo.findAllByFileId(file._id)
        val numberOfChunkAfterSecondMessage = chunkListAfterSecondMessage.size.toLong()
        Assertions.assertEquals(numberOfChunkBefore+3, numberOfChunkAfterSecondMessage) {
            "The number of chunks is $numberOfChunkAfterSecondMessage and not 3"
        }
        /** No death chunks are left. **/
        Assertions.assertEquals(numberOfChunkAfter + 1, numberOfChunkAfterSecondMessage) {
            "The number of chunks is $numberOfChunkAfterSecondMessage and not 3"
        }

        /** First updating failed after creating 2 of 3 of the chunks contained in the message.
         * So the second update doesn't update or create the first 2 document in chunk collection, create only the last one **/
        val chunkListAfterFirstMessageId = chunkListAfterFirstMessage.map { it._id }
        val chunkListAfterSecondMessageId = chunkListAfterSecondMessage.map { it._id }
        Assertions.assertTrue(chunkListAfterSecondMessageId.containsAll(chunkListAfterFirstMessageId)) {
            "The chunk list after the second update doesn't contain all the chunks id created by the first - interrupted - update."
        }
        val afterInterruptionLengthSecondMessage = chunkListAfterSecondMessage.flatMap { it.data.toList() }.size.toLong()
        Assertions.assertEquals(file.length + bytesToAppendMessage.size.toLong(), afterInterruptionLengthSecondMessage) {
            "The new size of all chunks is $afterInterruptionLengthSecondMessage and not ${file.length + bytesToAppendMessage.size.toLong()}"
        }
        val fileAfterSecondMessage = fileRepo.findByFilename(filename)
        Assertions.assertEquals(file.length, fileAfterSecondMessage.length) {
            "The size of the file is ${fileAfterSecondMessage.length} and not ${file.length}. Length is change."
        }
        Assertions.assertEquals(file.lastOffset, fileAfterSecondMessage.lastOffset) {
            "The lastOffset of the file is ${fileAfterSecondMessage.lastOffset} and not ${file.lastOffset}. LastOffset is change."
        }
        val fileAfterRetrievedSecondMessage = fileService.getEntireResource(filename)
        Assertions.assertArrayEquals(byteArrayOf(), fileAfterRetrievedSecondMessage.data) {
            "The content of the file retrieved is different from the initial (empty) content."
        }
        /** Updated not interrupted, document in file collection is updated **/
        fileRepo.findAndSetLastOffsetAndIncrementLengthBy_id(file._id, file.lastOffset+1, bytesToAppendMessage.size.toLong())
        val fileAfterRetrievedUpdated = fileService.getEntireResource(filename)

        Assertions.assertEquals(file.length + bytesToAppendMessage.size.toLong(), fileAfterRetrievedUpdated.file.length) {
            "The size of the file is ${fileAfterRetrievedUpdated.file.length} and not ${file.length + bytesToAppendMessage.size.toLong()}."
        }
        Assertions.assertEquals(file.lastOffset + 1 , fileAfterRetrievedUpdated.file.lastOffset) {
            "The lastOffset of the file is ${fileAfterRetrievedUpdated.file.lastOffset} and not ${file.lastOffset + 1}."
        }
        Assertions.assertArrayEquals(bytesToAppendMessage, fileAfterRetrievedUpdated.data) {
            "The content of the file retrieved is different from the updated one expected."
        }
    }

}