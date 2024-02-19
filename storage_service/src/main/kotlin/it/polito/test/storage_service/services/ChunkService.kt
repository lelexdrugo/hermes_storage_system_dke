package it.polito.test.storage_service.services

import it.polito.test.storage_service.documents.Chunk
import it.polito.test.storage_service.documents.FileDocument
import org.bson.types.ObjectId
import java.io.InputStream

interface ChunkService {
    /**
     * Retrieve the list of chunks, related to a file, containing the data between the start byte and the end byte specified as parameters
     * The range is inclusive.
     * @param fileId the id of the file
     * @param start the start byte
     * @param end the end byte
     * */
    fun getSpecificChunks(fileId: ObjectId, start: Int, end: Int): List<Chunk>

    /**
     * Append data to a file already stored.
     * It updates the list of chunks, adding new ones if needed and updating the last one if it is not complete.
     * These operation are done through mongo bulk write to improve performance.
     * The method is idempotent and fault-tolerant.
     * @param file the file to update
     * @param fileId the id of the file
     * @param data the content to append
     * @param contentLength the length of the data to append
     */
    fun appendData(file: FileDocument, fileId: ObjectId, data: InputStream, contentLength: Long): Long

    /*
     * Used in test or debug function
     * Could lead to OOM error
     */
    fun getAllChunksOfAFile(fileId: ObjectId): List<Chunk>
}
