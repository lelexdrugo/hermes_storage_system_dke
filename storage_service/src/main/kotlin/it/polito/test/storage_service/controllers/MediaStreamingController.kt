package it.polito.test.storage_service.controllers

import it.polito.test.storage_service.documents.FileDocument
import it.polito.test.storage_service.exceptions.FileNotFoundException
import it.polito.test.storage_service.services.FileService
import it.polito.test.storage_service.services.MediaStreamLoader
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Query
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.IOException

@RestController
@RequestMapping("media")
class MediaStreamingController {
    @Autowired
    lateinit var fileService : FileService
    @Autowired
    private lateinit var mongoDb: MongoTemplate
    @Autowired
    private lateinit var mediaLoaderService: MediaStreamLoader

    private val logger = KotlinLogging.logger {}

    /**
     *  Method to save a file uploaded with a multipart request.
     *  Delegate to FileService the creation of the persistence of the file into the underlying data layer.
     *  Each parameter annotated with @RequestParam corresponds to the "name" of each of the part of a multipart request.
     *  i.e. Content-Disposition: form-data; name="file" filename="originalFileName" -- for the part with the binary file
     *  Content-Disposition: form-data; name="name" -- for the part with the new name of the file
     *  Each part could specify different Content-Type
     */
    @PostMapping(consumes =  ["multipart/form-data"])
    fun uploadFile(
        @RequestParam("file") file: MultipartFile,
        @RequestParam("name") name: String,
    ): ResponseEntity<String> {
        logger.info { "Saving file: $name" }
        return try {
            val inputStream = file.inputStream
            val contentType = file.contentType?: "void/void"
            val timeBefore = System.currentTimeMillis()
            fileService.createFile(name, inputStream, contentType, -1, file.size)
            logger.trace{"File: $name saved on disk in: ${System.currentTimeMillis()-timeBefore} ms"}
            ResponseEntity.ok("Uploaded file saved successfully.")
        } catch (e: Exception) {
            println("Error while creating file: ${e.message}")
            ResponseEntity(HttpStatus.BAD_REQUEST)
        }
    }

    /**
     * Method to retrieve a file in a streaming way, compliantly to the HTTP Range request possibility (RFC7233)
     * https://www.rfc-editor.org/rfc/rfc7233
     * The method is called by the browser when the user clicks on the video tag.
     * The browser could send a request specifying a header, even without end of the range
     * i.e. "Range: bytes=0-"
     * @param filename the name of the file to retrieve
     * @param rangeHeader header of the request
     * @return ResponseEntity<StreamingResponseBody> which is a Spring class that allows to stream the file to the client
     */
    @GetMapping("{name}")
    @ResponseBody
    fun getFileStreaming(
        @PathVariable("name") filename: String,
        @RequestHeader(value = "Range", required = false) rangeHeader: String?,
    ): ResponseEntity<StreamingResponseBody> {
        return try {
            logger.info { "Requesting file $filename" }
            mediaLoaderService.loadPartialMediaFile(filename, rangeHeader)
        } catch (e: FileNotFoundException) {
            logger.warn("File not found: ${e.message}")
            ResponseEntity(HttpStatus.NOT_FOUND)
        } catch (e: IOException) {
            logger.error("Error while reading file: ${e.message}")
            ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    /**
     * Method to retrieve all the filename of the files stored in the database.
     * Useful to show the list of the files in the frontend
     * @return ResponseEntity<List<String>> with the list of the filename
     */
    @GetMapping("all")
    fun getAllMediaName(): ResponseEntity<List<String>> {
        val q = Query()
        q.fields().include("filename")   //This performs projection. Otherwise, I must create a class projection, annotated with collection main class
        val listOfFilename = mongoDb.find(q, FileDocument::class.java).map { it.filename }
        return ResponseEntity.ok(listOfFilename)
    }
}