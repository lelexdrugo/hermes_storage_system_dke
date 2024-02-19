package it.polito.test.storage_service.services

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.util.StringUtils
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.IOException

@Service
class MediaStreamLoaderImpl(val fileServiceImpl: FileService): MediaStreamLoader {

    @Throws(IOException::class)
    override fun loadEntireMediaFile(filename: String): ResponseEntity<StreamingResponseBody> {
        // I use my service instead of File api by java
        // This can throw exception, handled by controller
        val fileDTO = fileServiceImpl.getFileInfo(filename)
        val fileSize = fileDTO.length

        val endPos = if (fileSize > 0L) {
            fileSize - 1
        } else {
            0L
        }
        return loadPartialMediaFile(filename, fileSize, 0, endPos, fileDTO.contentType)
    }

    @Throws(IOException::class)
    override fun loadPartialMediaFile(filename: String, rangeValues: String?): ResponseEntity<StreamingResponseBody> {
        return if (!StringUtils.hasText(rangeValues)) {
            println("Read all media file content.")
            loadEntireMediaFile(filename)
        } else {
            rangeValues!!
            var rangeStart = 0L
            var rangeEnd = 0L
            //I have validation, so this should not happen
            require(StringUtils.hasText(filename)) { "The full path to the media file is NULL or empty." }


            // I use my service instead of File api by java
            // This can throw exception, handled by controller
            val fileDTO = fileServiceImpl.getFileInfo(filename)
            val fileSize = fileDTO.length

            println("Read range seeking value.")
            println("Rang values: [$rangeValues]")
            val dashPos = rangeValues.indexOf("-")
            if (dashPos > 0 && dashPos <= rangeValues.length - 1) {
                val rangesArr = rangeValues.split("-".toRegex())/*.dropLastWhile { it.isEmpty() }
                    .toTypedArray()*/
                if (rangesArr.isNotEmpty()) {
                    println("ArraySize: " + rangesArr.size)
                    rangeStart = if (StringUtils.hasText(rangesArr[0])) {
                        println("Rang values[0]: [" + rangesArr[0] + "]")
                        val valToParse = numericStringValue(rangesArr[0])
                        safeParseStringValuetoLong(valToParse)
                    } else {
                        0L
                    }
                    rangeEnd = if (rangesArr.size > 1) {
                        println("Rang values[1]: [" + rangesArr[1] + "]")
                        val valToParse = numericStringValue(rangesArr[1])
                        safeParseStringValuetoLong(valToParse)
                    } else {
                        if (fileSize > 0) {
                            fileSize - 1L
                        } else {
                            0L
                        }
                    }
                }
            }
            if (rangeEnd == 0L && fileSize > 0L) {
                rangeEnd = fileSize - 1
            }
            if (fileSize < rangeEnd) {
                rangeEnd = fileSize - 1
            }
            println(String.format("Parsed Range Values: [%d] - [%d]", rangeStart, rangeEnd))
            loadPartialMediaFile(filename, fileSize, rangeStart, rangeEnd, fileDTO.contentType)
        }
    }

    /**
     * This has to be called only by other function of this service. For example check on the file existence is done by the caller.
     */
    @Throws(IOException::class)
    override fun loadPartialMediaFile(
        filename: String,
        fileSize: Long,
        fileStartPos_: Long,
        fileEndPos_: Long,
        contentType: String
        ): ResponseEntity<StreamingResponseBody> {
        var fileStartPos = fileStartPos_
        var fileEndPos = fileEndPos_

        val responseStream: StreamingResponseBody
        var httpStatus = HttpStatus.PARTIAL_CONTENT
        if(fileStartPos == 0L && (fileEndPos == (fileSize -1L)))
            httpStatus = HttpStatus.PARTIAL_CONTENT  //Manage the case in which the file is requested with range 0-. It has to be PARTIAL_CONTENT
        //Some extra check
        if (fileStartPos < 0L) {
            fileStartPos = 0L
        }
        if (fileSize > 0L) {
            if (fileStartPos >= fileSize) {
                fileStartPos = fileSize - 1L
            }
            if (fileEndPos >= fileSize) {
                fileEndPos = fileSize - 1L
            }
        } else {
            fileStartPos = 0L
            fileEndPos = 0L
        }

        val mimeType = contentType//"video/mp4" /*"text/plain"*/
//        val mimeType = Files.probeContentType(filePath)
        val responseHeaders = HttpHeaders()
        val contentLength = (fileEndPos - fileStartPos + 1).toString()
        responseHeaders.add("Content-Type", mimeType)
        responseHeaders.add("Content-Length", contentLength)
        responseHeaders.add("Accept-Ranges", "bytes")
        //I want to stream growing files... Try to return a fake file size to fool the player
        //With the check before I have to be able to manage request out of range
        responseHeaders.add("Content-Range", String.format("bytes %d-%d/%d", fileStartPos, fileEndPos, fileSize))
        println("Known file size: $fileSize")
        val fakeFileSize = fileSize + 1000000000
//        println("Fake file size: $fakeFileSize")
//        responseHeaders.add("Content-Range", String.format("bytes %d-%d/%d", fileStartPos, fileEndPos, fakeFileSize))

        val unknownFileSize= "*"
//        responseHeaders.add("Content-Range", String.format("bytes %d-%d/$unknownFileSize", fileStartPos, fileEndPos))

        responseStream = fileServiceImpl.getResourceBlockStreaming(filename, fileStartPos, fileEndPos)
        return ResponseEntity(responseStream, responseHeaders,httpStatus)
    }

    private fun safeParseStringValuetoLong(valToParse: String, defaultVal: Long = 0): Long {
        var retVal = defaultVal
        if (StringUtils.hasText(valToParse)) {
            retVal = try {
                valToParse.toLong()
            } catch (ex: NumberFormatException) {
                // TODO: log the invalid long int val in text format.
                defaultVal
            }
        }
        return retVal
    }

    private fun numericStringValue(origVal: String): String {
        var retVal = ""
        if (StringUtils.hasText(origVal)) {
            retVal = origVal.replace("[^0-9]".toRegex(), "")
            println("Parsed Long Int Value: [$retVal]")
        }
        return retVal
    }



}