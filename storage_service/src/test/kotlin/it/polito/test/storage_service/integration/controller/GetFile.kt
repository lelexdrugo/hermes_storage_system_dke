package it.polito.test.storage_service.integration.controller

import it.polito.test.storage_service.controllers.ErrorDetails
import it.polito.test.storage_service.dtos.FileResourceBlockDTO
import it.polito.test.storage_service.repositories.ChunkRepository
import it.polito.test.storage_service.repositories.FileRepository
import it.polito.test.storage_service.services.FileService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.web.client.exchange
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import kotlin.random.Random

class GetFile: ReadAPI() {

    @Autowired
    private lateinit var fileRepo: FileRepository

    @Autowired
    private lateinit var chunkRepo: ChunkRepository

    @Autowired
    private lateinit var fileService : FileService

    val CHUNK_SIZE = 261120

    val CONTENT_SIZE = 10*CHUNK_SIZE
    //val INITIAL_FILE_SIZE = CHUNK_SIZE/2
    val INITIAL_FILE_SIZE =CONTENT_SIZE
    val contentByteArray = ByteArray(CONTENT_SIZE).also { java.util.Random().nextBytes(it) }
    val fileContent = contentByteArray.take(INITIAL_FILE_SIZE).toByteArray()
    val metadata = mutableMapOf("contentType" to "document/docx")

    // The file creation in the setup could become a message injection in a kafka container
    @BeforeEach
    fun setUp() {
        fileRepo.deleteAll()
        chunkRepo.deleteAll()
        val defaultOffset = -1L
        fileService.createFile(filename, fileContent.inputStream(), metadata["contentType"]!!, defaultOffset, INITIAL_FILE_SIZE.toLong())
        val file = fileRepo.findByFilename(filename)
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
    fun `read an existing file`(){
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}",
            HttpMethod.GET,
        )

        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            INITIAL_FILE_SIZE.toLong(),
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertArrayEquals(fileContent, response.body!!.data)
    }

    @Test
    fun `read a file that doesn't exist`(){
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${"fake"+filename}",
            HttpMethod.GET,
        )
        Assertions.assertNull(response.body, "The response is not null")
        Assertions.assertEquals(
            HttpStatus.NOT_FOUND,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.NOT_FOUND}"
        )
    }
    /**
     * Filename parameter
     */

    @Test
    fun `read with empty filename`(){
        val response: ResponseEntity<ErrorDetails> =restTemplate.exchange(
            "/files/?filename=${""}",
            HttpMethod.GET,
        )

        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.BAD_REQUEST,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.BAD_REQUEST}"
        )
    }

    /**
     * BlockSize parameter
     */

    @Test
    fun `read a file with negative blockSize`(){
        val response: ResponseEntity<ErrorDetails> =restTemplate.exchange(
            "/files/?filename=${filename}&blockSize=${-1}",
            HttpMethod.GET,
        )

        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.BAD_REQUEST,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.BAD_REQUEST}"
        )
    }

    @Test
    fun `read a file with blockSize equal to 0`(){
        val response: ResponseEntity<ErrorDetails> =restTemplate.exchange(
            "/files/?filename=${filename}&blockSize=${0}",
            HttpMethod.GET,
        )

        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.BAD_REQUEST,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.BAD_REQUEST}"
        )
    }

    @Test
    fun `read a file with blockSize greater than the file size`(){
        val blockSize = INITIAL_FILE_SIZE+1
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&blockSize=$blockSize",
            HttpMethod.GET,
        )

        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            INITIAL_FILE_SIZE.toLong(),
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(blockSize, response.body!!.requestedBlockSize, "The requested block size is not equal to $blockSize")
        Assertions.assertEquals(INITIAL_FILE_SIZE, response.body!!.returnedBlockSize, "The returned block size is not equal to $INITIAL_FILE_SIZE")
        Assertions.assertArrayEquals(fileContent, response.body!!.data)
    }

    @Test
    fun `read a file with blockSize lower than the file size`(){
        val blockSize = INITIAL_FILE_SIZE/2
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&blockSize=$blockSize",
            HttpMethod.GET,
        )

        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            blockSize.toLong(),
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(blockSize, response.body!!.requestedBlockSize, "The requested block size is not equal to $blockSize")
        Assertions.assertEquals(blockSize, response.body!!.returnedBlockSize, "The returned block size is not equal to $blockSize")
        Assertions.assertArrayEquals(fileContent.take(blockSize).toByteArray(), response.body!!.data)
    }

    /**
     * Offset parameter
     */
    @Test
    fun `read a file with offset`(){
        val offset = Random.nextInt(1, INITIAL_FILE_SIZE)
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&offset=$offset",
            HttpMethod.GET,
        )
        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            INITIAL_FILE_SIZE.toLong()-offset.toLong(),
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(0, response.body!!.requestedBlockSize, "The requested block size is not equal to 0")
        Assertions.assertEquals(INITIAL_FILE_SIZE-offset, response.body!!.returnedBlockSize, "The returned block size is not equal to $INITIAL_FILE_SIZE")
        Assertions.assertArrayEquals(fileContent.drop(offset).toByteArray(), response.body!!.data)
    }

    @Test
    fun `read a file with offset equal to -1`(){
        val offset = -1L
        val response: ResponseEntity<ErrorDetails> =restTemplate.exchange(
            "/files/?filename=${filename}&offset=$offset",
            HttpMethod.GET,
        )
        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.BAD_REQUEST,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.BAD_REQUEST}"
        )
    }

    @Test
    fun `read a file with offset equal to 0`(){
        val offset = 0L
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&offset=$offset",
            HttpMethod.GET,
        )
        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            INITIAL_FILE_SIZE.toLong(),
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(0, response.body!!.requestedBlockSize, "The requested block size is not equal to 0")
        Assertions.assertEquals(INITIAL_FILE_SIZE, response.body!!.returnedBlockSize, "The returned block size is not equal to $INITIAL_FILE_SIZE")
        Assertions.assertArrayEquals(fileContent, response.body!!.data)
    }

    @Test
    fun `read a file with offset equal to the file size`(){
        val offset = INITIAL_FILE_SIZE
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&offset=$offset",
            HttpMethod.GET,
        )
        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            0L,
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(0, response.body!!.requestedBlockSize, "The requested block size is not equal to $INITIAL_FILE_SIZE")
        Assertions.assertEquals(0, response.body!!.returnedBlockSize, "The returned block size is not equal to $INITIAL_FILE_SIZE")
        Assertions.assertArrayEquals(byteArrayOf(), response.body!!.data)
    }

    @Test
    fun `read a file with offset equal to 0 is equal to not specify it`(){
        val response1: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}",
            HttpMethod.GET,
        )
        val response2: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&offset=0",
            HttpMethod.GET,
        )
        Assertions.assertNotNull(response1.body, "The response is null")
        Assertions.assertNotNull(response2.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response1.statusCode,
            "Status code is ${response1.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            HttpStatus.OK,
            response2.statusCode,
            "Status code is ${response2.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            response1.body!!.data.size.toLong(),
            response2.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(response1.body!!.file.filename, response2.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(response1.body!!.file.length, response2.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            response1.body!!.file.metadata["_contentType"],
            response2.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(response1.body!!.requestedBlockSize, response2.body!!.requestedBlockSize, "The requested block size is not equal to $INITIAL_FILE_SIZE")
        Assertions.assertEquals(response1.body!!.returnedBlockSize, response2.body!!.returnedBlockSize, "The returned block size is not equal to $INITIAL_FILE_SIZE")
        Assertions.assertArrayEquals(response1.body!!.data, response2.body!!.data)
    }

    /**
     * Offset and blockSize parameters
     */
    @Test
    fun `read a file with offset and blockSize`(){
        val offset = INITIAL_FILE_SIZE/2
        val blockSize = INITIAL_FILE_SIZE/4
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&offset=$offset&blockSize=$blockSize",
            HttpMethod.GET,
        )
        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            blockSize.toLong(),
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(blockSize, response.body!!.requestedBlockSize, "The requested block size is not equal to $blockSize")
        Assertions.assertEquals(blockSize, response.body!!.returnedBlockSize, "The returned block size is not equal to $blockSize")
        Assertions.assertArrayEquals(fileContent.copyOfRange(offset, offset+blockSize), response.body!!.data)
    }

    @Test
    fun `read a file with blockSize greater than file size minus offset`(){
        val offset = INITIAL_FILE_SIZE*2/3
        val blockSize = INITIAL_FILE_SIZE/2
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=${filename}&offset=$offset&blockSize=$blockSize",
            HttpMethod.GET,
        )
        Assertions.assertNotNull(response.body, "The response is null")
        Assertions.assertEquals(
            HttpStatus.OK,
            response.statusCode,
            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
        )
        Assertions.assertEquals(
            INITIAL_FILE_SIZE-offset.toLong(),
            response.body!!.data.size.toLong(),
            "The size of the file is not the same"
        )
        Assertions.assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        Assertions.assertEquals(INITIAL_FILE_SIZE.toLong(), response.body!!.file.length, "The length of the file is not the same")
        Assertions.assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        Assertions.assertEquals(blockSize, response.body!!.requestedBlockSize, "The requested block size is not equal to $blockSize")
        Assertions.assertEquals(INITIAL_FILE_SIZE-offset, response.body!!.returnedBlockSize, "The returned block size is not equal to $blockSize")
        Assertions.assertArrayEquals(fileContent.drop(offset).toByteArray(), response.body!!.data)
    }
}