package it.polito.test.storage_service.controllers

import com.mongodb.DBObject
import it.polito.test.storage_service.dtos.*
import it.polito.test.storage_service.documents.Chunk
import it.polito.test.storage_service.exceptions.FileNotFoundException
import it.polito.test.storage_service.services.ChunkService
import it.polito.test.storage_service.services.FileService
import mu.KotlinLogging
import org.bson.types.ObjectId
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import org.springframework.validation.BindingResult
import org.springframework.validation.FieldError
import org.springframework.validation.ObjectError
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.validation.Valid
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.Positive

@Validated
@RestController
class Controller {
    @Autowired
    private lateinit var chunkService: ChunkService
    @Autowired
    private lateinit var fileService: FileService

    @Autowired
    private lateinit var mongoDb: MongoTemplate

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, Message<Any>>

    private val logger = KotlinLogging.logger {}


    /**
     * Endpoint to retrieve a file.
     * It's possible to read a single block of given dimension and start reading from a specific offset.
     * Useful for block by block reading and seek option
     * @param filename: REQUIRED. Client must provide the identifier of the file he wants to read.
     * @param blockSize: OPTIONAL. Client could provide the size of the portion of file to read. Otherwise, service will return the entire file. If offset is provided, it will return a portion of blocksize starting from offset
     * @param offset: OPTIONAL. Client could specify the offset of the file to read (e.g. seek operation). If a blocksize is specified, it will return a portion of blocksize starting from offset; otherwise it will return the entire file from beginning
     * */
    @GetMapping("/files/")
    fun getFile(
        @RequestParam(name = "filename", required = true) @NotBlank filename: String,
        @RequestParam(name = "blockSize", required = false) @Positive blockSize: Int?,
        @RequestParam(name = "offset", required = false) @Min(0) offset: Int?): ResponseEntity<FileResourceBlockDTO> {
        return try {
            val fileResourceBlockDTO =  fileService.getResourceBlock(filename,blockSize,offset)
            ResponseEntity(fileResourceBlockDTO, HttpStatus.OK)
        }catch (e: FileNotFoundException){
            logger.debug("File not found: ${e.message}")
            ResponseEntity(HttpStatus.NOT_FOUND)
        }
        catch (e: Exception) {
            logger.error("Error while reading file: ${e.message}"+
                    "${e.stackTrace}")
            ResponseEntity(HttpStatus.BAD_REQUEST)
        }
    }
    /**
     * Method to retrieve a file block by block
     * @param filename: client must provide the identifier of the file he wants to read. It could be also the fileId
     * @param blockSize: client could provide the size of the portion of file to read. Otherwise, service will choose the more appropriate (e.g. block size)
     * @param offset: client could provide the offset of the file to read (e.g. seek operation). Otherwise, service will return the first portion of file
     * */
    @GetMapping("/files/optimized")
    fun getFileOptimizedVersion(
        @RequestParam(name = "filename", required = true) filename: String,
        @RequestParam(name = "blockSize", required = false) blockSize: Int?,
        @RequestParam(name = "offset", required = false) offset: Int?): ResponseEntity<FileResourceBlockDTO> {
        return try {
            val fileResourceBlockDTO =  fileService.getResourceBlockOptimizedVersion(filename,blockSize,offset)
            ResponseEntity(fileResourceBlockDTO, HttpStatus.OK)
        } catch (e: FileNotFoundException){
            logger.warn("File not found: ${e.message}")
            ResponseEntity(HttpStatus.NOT_FOUND)
        } catch (e: Exception) {
            logger.error("Error while reading file: ${e.message}")
            ResponseEntity(HttpStatus.BAD_REQUEST)
        }
    }

    /**
     * Method to create a file. This could be useful in case we need a synchronous REST endpoint
     * if we decide to force who wants to store data to first call this method to ensure that filename chosen is unique
     * Like an open filesystem api, followed by a write operation
     * @param name: client must provide the file name. Must be unique
     * @body file: a FileToCreateDTO. Client could provide a content for the file and the content type
     */
    @PostMapping("/files/{name}/")
    fun createAFile(@PathVariable("name") name: String,
                    @Valid @RequestBody file: FileToCreateDTO,
                    bindingResult: BindingResult
    ): ResponseEntity<FileDTO> {
        if (bindingResult.hasErrors()) {
            logBindingResultErrors(bindingResult)
            return ResponseEntity(HttpStatus.BAD_REQUEST)
        }
        return try {
            fileService.createFile(name, file.fileContent.toByteArray().inputStream(), file.contentType, -1, file.fileContent.toByteArray().size.toLong())
            val fileDTO = fileService.getFileInfo(name)
            ResponseEntity(fileDTO, HttpStatus.CREATED)
        } catch (e: Exception) {
            logger.error("Error while creating file: ${e.message}")
            ResponseEntity(HttpStatus.BAD_REQUEST)
        }
    }

    /**
     * Method to update a file. This method is a facility that mock up the real future use.
     * It simply pushes a message to Kafka topic, used by the fileService.
     * It could be substituted by a direct call to a method similar to sendMessageToUpdateFile
     * @body dataToStoreDTO: a DataToStoreDTO. Client must provide the chunk to store
     */
    @PutMapping("/files/")
    fun updateAFile(@Valid @RequestBody dataToStoreDTO: DataToStoreDTO,
                    bindingResult: BindingResult
    ): ResponseEntity<Unit> {
        // bindingResult is automatically populated by Spring validate, trying to parse a DataToAddDTO which
        // respects the validation annotations in ChunkDTO. These errors can be detected before trying to insert into
        // the db
        if (bindingResult.hasErrors()) {
            // If the json contained in the put body does not satisfy our validation annotations, return 400
            //Can be used for debugging, to extract the exact errors
            logBindingResultErrors(bindingResult)
            return ResponseEntity(HttpStatus.BAD_REQUEST)
        }

        return try {
            //chunkService.addChunkToAFile(chunkDTO)
            sendMessageToUpdateFile(dataToStoreDTO)
            ResponseEntity(HttpStatus.OK)
        } catch (ex: Exception) { // Uniqueness exceptions (known only after trying to insert into database)
            //the exceptions should be DataIntegrityException and RegistrationException
            logger.error { "\tAdd chunks not valid: ${ex.message}" }
            ResponseEntity(HttpStatus.BAD_REQUEST)
        }

    }

    @GetMapping("/chunks/file/{fileId}")
    fun getAllChunksByFile(@PathVariable("fileId") fileId: ObjectId): ResponseEntity<List<Chunk>> {
        val listOfChunks = chunkService.getAllChunksOfAFile(fileId)
        return ResponseEntity.ok(listOfChunks.map { it })
    }

    @GetMapping("/collection/{name}")
    fun getEntireCollection(@PathVariable name: String): List<DBObject?>? {
        return mongoDb.findAll(DBObject::class.java, name)
    }


    private fun sendMessageToUpdateFile(dataToStoreDTO: DataToStoreDTO) {
        logger.info { "Sending message to file service to update data" }
        val kafkaMessageDTO = KafkaPayloadDTO(dataToStoreDTO, "Update file")
        val message: Message<KafkaPayloadDTO> = MessageBuilder
            .withPayload(kafkaMessageDTO)
            .setHeader(KafkaHeaders.TOPIC, "fileStorage")
            .build()
        kafkaTemplate.send(message)
        logger.info { "Message sent to file service" }
    }
    fun logBindingResultErrors(bindingResult: BindingResult) {
        val errors: MutableMap<String, String?> = HashMap()
        bindingResult.allErrors.forEach { error: ObjectError ->
            val fieldName = (error as FieldError).field
            val errorMessage = error.getDefaultMessage()
            errors[fieldName] = errorMessage
        }
        logger.error { errors }
    }
}



