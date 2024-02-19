package it.polito.test.storage_service.services

import org.springframework.http.ResponseEntity
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.IOException

interface MediaStreamLoader {
    /**
     * Obtain the media file content, in a streaming way, using the range values specified as parameter.
     * The range is inclusive.
     * If Range Request header is absent, rangeValues is null. In that case the entire file is loaded.
     * @param filename the name of the file to load
     * @param rangeValues the range values, in the format "bytes=0-100"
     * @return the response entity containing the streaming body and the header
     * @throws IOException
     */
    @Throws(IOException::class)
    fun loadPartialMediaFile(filename: String, rangeValues: String?): ResponseEntity<StreamingResponseBody>

    /**
     * Obtain the media file content, in a streaming way, requesting chunks included between fileStartPos and fileEndPos.
     * Generate the header of the response entity containing the content type and the content range, using information retrieved from the file document
     * @param filename the name of the file to load
     * @param fileSize the size of the file to load
     * @param fileStartPos the start position of the file to load
     * @param fileEndPos the end position of the file to load
     * @param contentType the content type of the file to load
     * @return the response entity containing the streaming body and the header
     * @throws IOException
     */
    @Throws(IOException::class)
    fun loadPartialMediaFile(
        filename: String,
        fileSize: Long,
        fileStartPos: Long,
        fileEndPos: Long,
        contentType: String
    ): ResponseEntity<StreamingResponseBody>

    /**
     * Obtain the media file content, in a streaming way, requesting the entire file.
     * Used when the Range Request header is absent.
     * @param filename the name of the file to load
     * @return the response entity containing the streaming body and the header
     * @throws IOException
     */
    @Throws(IOException::class)
    fun loadEntireMediaFile(filename: String): ResponseEntity<StreamingResponseBody>

}