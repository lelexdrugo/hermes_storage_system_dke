package it.polito.test.storage_service.services

import it.polito.test.storage_service.documents.Chunk
import it.polito.test.storage_service.documents.FileDocument
import it.polito.test.storage_service.exceptions.ChunkException
import it.polito.test.storage_service.repositories.ChunkRepository
import it.polito.test.storage_service.repositories.FileRepository
import mu.KotlinLogging
import org.bson.Document
import org.bson.types.ObjectId
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.stereotype.Service
import java.io.InputStream
import kotlin.ByteArray
import kotlin.Exception
import kotlin.Int
import kotlin.Long


@Service
class ChunkServiceImpl(val chunkRepo: ChunkRepository, val fileRepo: FileRepository, val gridFs: GridFsTemplate, val mongoTemplate: MongoTemplate) : ChunkService {
    @Value("\${bulk-write}")
    var bulkWrite: Boolean = false

    private val logger = KotlinLogging.logger {}

    /**********************************
     * Retrieving API
     ********************************/

    override fun getSpecificChunks(fileId: ObjectId, start: Int, end: Int): List<Chunk> {
        val chunkSize = fileRepo.findBy_id(fileId).chunkSize
        val firstN = (start / chunkSize).toInt()
        val lastN = (end / chunkSize).toInt()
        //Method is exclusive, so I must add and subtract 1
//        return chunkRepo.findAllByFileIdAndNBetween(fileId, firstN-1, lastN+1)
        /*        if(start == end)
                    return listOf()*/
        return chunkRepo.findAllByFileIdAndNBetweenInclusive(fileId, firstN, lastN)
    }

    /**
     * This method retrieve all the chunks, even if they are stored and the length of the file is not updated
     * This could lead to OOM if the file is too big
     */
    override fun getAllChunksOfAFile(fileId: ObjectId): List<Chunk> {
        return chunkRepo.findAllByFileId(fileId)
    }

    /**********************************
     * Writing API
     ********************************/

    /**
     * This method is used to append data to a file already stored
     * So it update the list of chunks, adding new ones if needed and updating the last one if it is not complete
     * These operation are done through mongo bulk write to improve performance.
     * The method is idempotent, so if it is called multiple times with the same parameters, it will not change the file (because file entity is the same).
     * This guarantee also that partial update of the file (it could happen even with bulk write, for this reason I choose Ordered one) will not be lost and will be completed when the method is called again.
     * So the method is idempotent and fault-tolerant.
     * Method iterate over the input stream, reading the data and writing it splitting it in chunks.
     * Appending starts from the last uncompleted chunk and could end with an incomplete chunk.
     * It couldn't happen that an error lead to the partial updating of a SINGLE document chunk.
     * In the first iteration the method check the consistence on the file length, to obtain fault tolerance and idempotency managing data.
     * Recovery from a lot of possible error is performed avoiding to recreate any already present chunk and managing accordingly remaining byte (if any) in the message being processed.
     */
    override fun appendData(file: FileDocument, fileId: ObjectId, data: InputStream, contentLength: Long) : Long {
        var numberOfAddedByte = 0L
        val initialSizeOfFile = file.length

        val bulkOperations = if(bulkWrite) mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, Chunk::class.java) else null
        val converter= if(bulkWrite) mongoTemplate.converter else null

        var firstTime = true

        val lastC: Chunk
        var actualNumberOfChunk: Int
        var realLength = 0L
        try {
            lastC = chunkRepo.findFirstByFileIdOrderByNDesc(file._id)
            realLength = (lastC.n * file.chunkSize) + lastC.data.size
            actualNumberOfChunk = lastC.n + 1
        } catch (e: EmptyResultDataAccessException) { //Change to not found exception
            logger.trace { "No chunk found. writing first one" }
            realLength = 0L
            actualNumberOfChunk = 0
        }

        var availableBytes = contentLength

        while(availableBytes>0) {
            //val fileLength = realLength//if (recursiveLength == 0L) file.length else recursiveLength
            //Situation in the last chunk
            val writtenBytes = realLength % file.chunkSize
            val emptySpace = file.chunkSize - writtenBytes
            val lastN = realLength / file.chunkSize //when writtenBytes == 0, lastN is the new one to use, otherwise is the index last one to complete

            if(firstTime) {
                firstTime = false
                //Bisogna gestire il caso in cui abbia già gestito del tutto o in parte questo messaggio, abbia magari creato dei document chunk e poi sia morto.
                //Per evitare di ricrearli e quindi avere in giro document con stesso n+fileId
                //Li cerco e continuo da dove era morto il precedente
                // "se i dati da appendere eccedono un chunk. Altrimenti sono certo che non succede nulla, sovrascriverò qualcosa di già scritto"
                //Verificare la cosa di cui sopra
                if (realLength != 0L    //Se esiste già qualche chunk
                    && realLength != file.length //Se c'è discrepanza tra le dimensioni del file e la somma delle dimensioni dei chunk
                    /*&& bytes.available() > emptySpace*/) {
                    logger.trace("Someone is died before")
                    //Check if appending was completed
                    if (realLength == file.length + availableBytes) {
                        logger.trace("Appending was completed")
                        numberOfAddedByte = realLength - initialSizeOfFile
                        return numberOfAddedByte
                    }
                    //Retrieve last chunk edited
                    //lastC is the last written chunk.
                    //Could happen that processing a message lead to the creation of more than one chunk.
                    //I don't manage creation of a chunk with partial data (creation is complete or rollback by mongo).
                    //So I have to manage when I must finish manage of a message that leads to creation of other chunks.
                    //I'm in the case of some existing chunk, so if lastN == 0 I must complete the first one
                    val writtenButNotCommittedBytes = realLength - file.length

//                    bytes.skipNBytes(writtenButNotCommittedBytes)
                    val skipRes = data.skip(writtenButNotCommittedBytes)
                    if(skipRes != writtenButNotCommittedBytes)
                        throw ChunkException("Error skipping bytes for a previous partial update. Reached end of stream before expected")
                    availableBytes -= writtenButNotCommittedBytes

                    //Non può succedere di riprendere da un chunk a metà: altrimenti sarei entrato nell'if di Appending was completed
                    //Questo perché solamente l'ultima write può portare a un chunk incompleto. Quindi avendo quella avrei già tutti i dati e potrei committare
                    val nFromWhichReStart = actualNumberOfChunk     //è già il numero, l'indice incrementato di uno
                    val bytesToStore =
                        if (availableBytes < file.chunkSize.toInt()) availableBytes else file.chunkSize

                    val byteToSave= ByteArray(bytesToStore.toInt())
                    val readRes = data.readNBytes(byteToSave, 0, bytesToStore.toInt())
                    if (readRes != bytesToStore.toInt()) {
                        logger.error { "Error reading bytes. Reached end of stream before expected. Bytes available: ${data.available()>0}; In node file length: ${file.length}; Real length: ${realLength}. Read value: $readRes; Content passed in stream: $contentLength; WrittenButNot: $writtenButNotCommittedBytes" }
                        throw ChunkException("Error reading bytes. Reached end of stream before expected")
                    }

                    availableBytes -= bytesToStore

                    realLength += bytesToStore //I don't have to add writtenButNotCommittedBytes, they are already in realLength
                    if(bulkWrite){
                        val c = Chunk(
                            fileId = file._id,
                            n = nFromWhichReStart,
                            data = byteToSave,
                            // lengthWithMe = realLength
                        )
                        val doc = Document()
                        converter!!.write(c, doc)
                        bulkOperations!!.insert(doc)
                    }
                    else{
                        chunkRepo.save(
                            Chunk(
                                fileId = file._id,
                                n = nFromWhichReStart,
                                data = byteToSave,
                                // lengthWithMe = realLength
                            )
                        )
                    }
                    continue
                }
            }
            //If writtenBytes == 0 it means that last chunk is full
            //So I must create at least a new chunk, even the first one
            if (writtenBytes.toInt() == 0) {
                val bufferSize = if (availableBytes < file.chunkSize.toInt()) availableBytes else file.chunkSize
                val newData = ByteArray(bufferSize.toInt())

                val res = data.readNBytes(newData, 0, bufferSize.toInt())
                if(res != bufferSize.toInt())
                    throw ChunkException("Error reading bytes. Reached end of the stream")

                availableBytes -= bufferSize
                realLength += bufferSize
                if (bulkWrite){
                    val c = Chunk(
                        fileId = file._id,
                        n = lastN.toInt(),
                        data = newData,
                    )

                    val doc = Document()
                    converter!!.write(c, doc)
                    bulkOperations!!.insert(doc)
                }
                else
                    chunkRepo.save(
                        Chunk(
                            fileId = file._id,
                            n = lastN.toInt(),
                            data = newData,
                        )
                    )

            } else {
                //Complete a chunk. This could happen only during first iteration of the function.
                // Then, I write only complete chunk. So this is compatible also with bulk write
                // There is not risk that findByFileIdAndN is called on a chunk that is not yet committed
                var chunk: Chunk
                try {
                    chunk = chunkRepo.findByFileIdAndN(fileId, lastN.toInt())
                }
                catch (e: Exception) {
                    logger.error("Error completing an existing chunk. Error: ${e.message}")
                    throw ChunkException("Error completing an existing chunk. Error: ${e.message}")
                }
                //val bufferSize = if (availableBytes < emptySpace) availableBytes else emptySpace.toInt()
                val bufferSize = if(chunk.data.size+ availableBytes < file.chunkSize.toInt()) availableBytes.toInt() else (file.chunkSize.toInt()-chunk.data.size)

                val newData = ByteArray(bufferSize)
                val readRes = data.readNBytes(newData, 0, bufferSize)
                if(readRes != bufferSize)
                    throw ChunkException("Error reading bytes. Reached end of the stream")

                availableBytes -= bufferSize
                realLength += bufferSize

                if (bulkWrite){
                    val finalData = chunk.data.plus(newData)

                    val doc = Document()
                    chunk.data = finalData
                    converter!!.write(chunk, doc)
                    bulkOperations!!.updateOne(
                        Query.query(Criteria.where("_id").`is`(chunk._id)),
                        Update.fromDocument(doc, "data").set("data", finalData))
                }
                val updateResult = chunkRepo.findAndSetDataBy_id(
                            chunk._id,
                            chunk.data.plus(newData)
                        )
            }
        }
        try {
            if (bulkWrite)
                bulkOperations!!.execute()
            numberOfAddedByte = realLength - initialSizeOfFile
            AssertionError("Error executing method").apply { if(contentLength != numberOfAddedByte) throw this }
            return numberOfAddedByte
        } catch (e: Exception) {
            throw ChunkException("Error updating chunks in batch: ${e.message}")
        }
    }

}