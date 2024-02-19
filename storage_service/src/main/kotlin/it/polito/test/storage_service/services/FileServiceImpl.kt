package it.polito.test.storage_service.services

import it.polito.test.storage_service.configurations.KafkaConsumerConfig
import it.polito.test.storage_service.configurations.MongoProperties
import it.polito.test.storage_service.documents.FileDocument
import it.polito.test.storage_service.dtos.*
import it.polito.test.storage_service.exceptions.FileException
import it.polito.test.storage_service.exceptions.FileNotFoundException
import it.polito.test.storage_service.repositories.FileRepository
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.bson.Document
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger


@Service
class FileServiceImpl(val fileRepo: FileRepository, val gridFsTemplate: GridFsTemplate, val mongoTemplate: MongoTemplate, val chunkService: ChunkService, val kafkaConsumerConfig: KafkaConsumerConfig) : FileService {
    private val logger = KotlinLogging.logger {}
    @Value("\${benchmarking}")
    val benchmarking: Boolean = false

    data class BenchmarkingData(val filename: String, val messageOffset: Long, val sendingInstant: Long, var receivedInstant: Long, var savingInstant: Long, var savedSize: Long, var latency: Long, var relativeLatency: Long, var assignedPartition: Int)
    val benchmarkingDataMap = ConcurrentHashMap<String, MutableList<BenchmarkingData>>()// mutableMapOf<String, MutableList<BenchmarkingData>>()
    val dumpedFile = AtomicInteger(0)
    /**********************************
     * LISTENERS
     ********************************/


    @KafkaListener(
        id = "oneByOneListener",
        topics = ["fileStorage"],
        autoStartup = "false",
        idIsGroup = false/*, groupId = "group_id_rcs"*/
    )
    override fun singleMessageConsumer(consumerRecord: ConsumerRecord<String, KafkaPayloadDTO>/*message: KafkaMessageDTO*/, ack: Acknowledgment) {
        val baseTime = kafkaConsumerConfig.listenerStartupTime
        val receivedInstant = System.currentTimeMillis()
        val currentOffset = consumerRecord.offset()
        val payload = consumerRecord.value()

        logger.trace{"Name of the thread consuming ${Thread.currentThread().name } \t partition: ${consumerRecord.partition()} \t  key: ${consumerRecord.key()}"}
    /*        val message = payload.message
        if(message == "initStats" || message == "logStats"){
            logDbStats("initStats")
        }*/
        val message = payload.message

        logger.trace("Consuming message: ${payload.message}, filename: ${payload.dataToStoreDTO.filename}, length: ${payload.dataToStoreDTO.fileContent.size}, metadata: ${payload.dataToStoreDTO.metadata}")
        logger.trace("Offset of the message: $currentOffset")

        val filename = payload.dataToStoreDTO.filename
        val contentLength = payload.dataToStoreDTO.fileContent.size.toLong()
        val fileContent: InputStream = payload.dataToStoreDTO.fileContent.inputStream()
        val metadata = payload.dataToStoreDTO.metadata

//        val (filename,  fileContent, metadata) = payload.dataToStoreDTO
        //Parse other metadata
        val contentType = metadata["contentType"] ?: "none"
        //Query mongoTemplate to find document
        val query = Query(Criteria.where("filename").`is`(filename))

        val file = this.mongoTemplate.findOne(query, FileDocument::class.java)// fileRepo.findByFilename(filename)
        if(file != null) {
            if(file.lastOffset>=currentOffset){
                logger.info("File already stored. Message already processed")
                ack.acknowledge()
                return
            }
            logger.trace("File already stored. Update by appending data with size: $contentLength")
            try {
                updateFile(file, fileContent, currentOffset, contentLength)
            }
            catch (e: Exception){
                logger.error("Error while updating file: $e")
                //This trigger ListenerExecutionFailedException, and force the consumer to restart from the last committed position
                throw FileException("Error while updating file: $e")
            }
        } else{
            logger.trace("File not stored. Create new file with size: $contentLength")
            //Of course, it's not necessary check for the last offset
            try {
                /*file = */createFile(filename = filename,
                    contentType = contentType,
                    fileContent = fileContent,
                    currentOffset = currentOffset,
                    contentLength = contentLength/*?:0*/)
            }
            catch (e: Exception){
                logger.error("Error while creating file: $e")
                throw FileException("Error while creating file: $e")
            }
        }
        ack.acknowledge()
        logger.trace("Message with offset: $currentOffset processed and committed")
    /*        if(message == "endStats" || message == "logStats"){
            logDbStats("endStats")
        }*/
//        if(benchmarking && message== "logTime") {
//            logTime(baseTime, filename)
//        }
        if(benchmarking) {
            val sendingInstant = consumerRecord.timestamp()
            val savingInstant = System.currentTimeMillis()
            val latency = savingInstant - sendingInstant
            val relativeLatency = savingInstant - baseTime
            val assignedPartition = consumerRecord.partition()
            benchmarkingDataMap
                .getOrPut(filename){ mutableListOf() }
                .add(BenchmarkingData(filename, currentOffset, sendingInstant, receivedInstant, savingInstant, contentLength, latency, relativeLatency, assignedPartition))
            if ( message == "logTime")
                logTimeForFile(benchmarkingDataMap[filename]!!)
        }
    }

    @KafkaListener(
        id = "batchAggregateMessageListener",
        topics = ["fileStorage"],
        autoStartup = "false",
        idIsGroup = false/*, groupId = "group_id_rcs"*/
    )
    override fun batchConsumerWithAggregation(listConsumerRecord: List<ConsumerRecord<String, KafkaPayloadDTO>>/*message: KafkaMessageDTO*/, ack: Acknowledgment) {
        val baseTime = kafkaConsumerConfig.listenerStartupTime
        val receivedInstant = System.currentTimeMillis()
        logger.trace ( "Number of records in batch listener: ${listConsumerRecord.size}" )
        data class AssociatedData(val offset: Long, val metadata: MutableMap<String,String>, var contentLength: Long, val containsLastMessage: Boolean)
        val mapOfAggregatedContent = mutableMapOf<String,SequenceInputStream>() //Manage metadata
        val mapOfAssociatedData = mutableMapOf<String,AssociatedData>() //Manage metadata
        listConsumerRecord.asSequence().forEach {
            mapOfAggregatedContent.merge(it.key(),
                SequenceInputStream((it.value().dataToStoreDTO.fileContent.inputStream()), InputStream.nullInputStream()))
                { old, new ->SequenceInputStream(old, new) }

//            mapOfAssociatedData.merge(it.key().toString(), Pair(it.offset(),it.value().dataToStoreDTO.fileContent.size.toLong())){ old, new -> Pair(it.offset(), old.second+new.second)}
            val containsLastMessage = it.value().message == "logTime"

            mapOfAssociatedData.merge(it.key(), AssociatedData(it.offset(), it.value().dataToStoreDTO.metadata, it.value().dataToStoreDTO.fileContent.size.toLong(), containsLastMessage)){ old, new -> new.contentLength+= old.contentLength; new}
            //To save time necessary to process all the message of a file... but it's not correct because sending time is in the end
//            if(benchmarking && it.value().message== "logTime") {
//                logTime(baseTime, it.key())
//            }
            if(benchmarking) {
                val filename = it.key()
                val currentOffset = it.offset()
                val sendingInstant = it.timestamp()
                benchmarkingDataMap
                    .getOrPut(filename){ mutableListOf() }
                    .add(BenchmarkingData(filename, currentOffset, sendingInstant, receivedInstant, -1, it.value().dataToStoreDTO.fileContent.size.toLong(), -1, -1, it.partition()))
            }
        }

        mapOfAggregatedContent.asSequence().forEach {
                aggregatedContent ->
            val filename = aggregatedContent.key

            val (cumulativeOffset, metadata, contentLength, containsLastMessage) = mapOfAssociatedData[filename]!!
            val contentType = metadata["contentType"] ?: "none"

            lateinit var file: FileDocument
            if(fileRepo.existsByFilename(filename)) {
                file = fileRepo.findByFilename(filename)
                if(file.lastOffset>=cumulativeOffset){
                    logger.info("File already stored. Message already processed")
                    val valueList=benchmarkingDataMap[filename]
                    val newList: MutableList<BenchmarkingData> = valueList?.map {
                        //Devo evitare di sovrascrivere i tempi di quelli presenti sulla mappa, ma non ancora dumpati (batch precedenti)
                        if(it.savingInstant != -1L)
                            return@map it
                        val savingInstant = System.currentTimeMillis()
                        val latency = savingInstant - it.sendingInstant
                        val relativeLatency = savingInstant - baseTime
                        it.savingInstant = savingInstant
                        it.latency = latency
                        it.relativeLatency = relativeLatency
                        it
                    }?.toMutableList() ?: mutableListOf()
                    benchmarkingDataMap[filename] = newList
                    if(benchmarking) {
                        if(containsLastMessage) {
                            logTimeForFile(benchmarkingDataMap[filename]!!)
                        }
                    }
                    //ack.acknowledge() //Ack the entire batch and terminate!!!! Assolutamente no!
                    return@forEach  //Continue with the next message
                }
                logger.trace("File already stored. Update by appending data with size")
                try {
                    updateFile(file, aggregatedContent.value, cumulativeOffset, contentLength)
                }
                catch (e: Exception){
                    logger.error("Error while updating file: $e")
                    //Terminate without ack
                    throw FileException("Error while updating file: $e")
                }
                val valueList=benchmarkingDataMap[filename]
                val newList: MutableList<BenchmarkingData> = valueList?.map {
                    if(it.savingInstant != -1L)
                        return@map it
                    val savingInstant = System.currentTimeMillis()
                    val latency = savingInstant - it.sendingInstant
                    val relativeLatency = savingInstant - baseTime
                    it.savingInstant = savingInstant
                    it.latency = latency
                    it.relativeLatency = relativeLatency
                    it
                }?.toMutableList() ?: mutableListOf()
                benchmarkingDataMap[filename] = newList
            } else{
                logger.trace("File not stored. Create new file with size")
                //Of course, it's not necessary check for the last offset
                try {
                     createFile(filename = filename,
                        contentType = contentType,
                        fileContent = aggregatedContent.value,
                        currentOffset = cumulativeOffset,
                        contentLength = contentLength)
                }
                catch (e: Exception){
                    logger.error("Error while creating file: $e")
                    //Terminate without ack
                    throw FileException("Error while creating file: $e")
                }
                val valueList=benchmarkingDataMap[filename]
                val newList: MutableList<BenchmarkingData> = valueList?.map {
                    val savingInstant = System.currentTimeMillis()
                    val latency = savingInstant-it.sendingInstant
                    val relativeLatency = savingInstant - baseTime
                    it.savingInstant = savingInstant
                    it.latency = latency
                    it.relativeLatency = relativeLatency
                    it
                }?.toMutableList() ?: mutableListOf()
                benchmarkingDataMap[filename] = newList
            }
            if(benchmarking) {
                if(containsLastMessage) {
                    logTimeForFile(benchmarkingDataMap[filename]!!)
                }
            }

        }
        ack.acknowledge()   //Ack the entire batch
    }
    @KafkaListener(
        id = "batchSimplyForwardListener",
        topics = ["fileStorage"],
        autoStartup = "false",
        idIsGroup = false/*, groupId = "group_id_rcs"*/
    )
    override fun batchConsumerSimplyForward(listConsumerRecord: List<ConsumerRecord<String, KafkaPayloadDTO>>/*message: KafkaMessageDTO*/, ack: Acknowledgment) {
            logger.trace { "Number of records in batch listener: ${listConsumerRecord.size}" }
            val baseTime = kafkaConsumerConfig.listenerStartupTime
            listConsumerRecord.asSequence().forEachIndexed {
                index, consumerRecord ->
            val currentOffset = consumerRecord.offset()
            val payload = consumerRecord.value()

            logger.trace { "Name of the thread consuming ${Thread.currentThread().name} \t partition: ${consumerRecord.partition()} \t  key: ${consumerRecord.key()}" }


            logger.trace("Consuming message: ${payload.message}, filename: ${payload.dataToStoreDTO.filename}, length: ${payload.dataToStoreDTO.fileContent.size}, metadata: ${payload.dataToStoreDTO.metadata}")
            logger.trace("Offset of the message: $currentOffset")

            val filename = payload.dataToStoreDTO.filename
            val contentLength = payload.dataToStoreDTO.fileContent.size.toLong()
            val fileContent: InputStream = payload.dataToStoreDTO.fileContent.inputStream()
            val metadata = payload.dataToStoreDTO.metadata
            val contentType = metadata["contentType"] ?: "none"

            val query = Query(Criteria.where("filename").`is`(filename))

            val file = this.mongoTemplate.findOne(query, FileDocument::class.java)// fileRepo.findByFilename(filename)
            if(file != null) {
                if(file.lastOffset>=currentOffset){
                    logger.info("File already stored. Message already processed")
                    //ack.acknowledge()
                    return@forEachIndexed  //Continue with the next message
                }
                logger.trace("File already stored. Update by appending data with size: $contentLength")
                try {
                    updateFile(file, fileContent, currentOffset, contentLength)
                }
                catch (e: Exception){
                    logger.error("Error while updating file: $e")
                    ack.nack(index/*currentOffset.toInt()*/, Duration.ZERO)//Partially commit batch and terminate
                    return
                    //throw FileException("Error while updating file: $e")
                }
            } else{
                logger.trace("File not stored. Create new file with size: $contentLength")
                //Of course, it's not necessary check for the last offset
                try {
                    createFile(filename = filename,
                        contentType = contentType,
                        fileContent = fileContent,
                        currentOffset = currentOffset,
                        contentLength = contentLength)
                }
                catch (e: Exception){
                    logger.error("Error while creating file: $e")
                    ack.nack(currentOffset.toInt(), Duration.ZERO)//Partially commit batch and terminate
                    return
                    //throw FileException("Error while creating file: $e")
                }
            }
            if(benchmarking && payload.message== "logTime") {
                logTime(baseTime, filename)
            }
        }
    //If everything was ok, ack the batch
    ack.acknowledge()
}

    /**********************************
     * Saving API
     ********************************/

   /**
    *  To obtain idempotent behaviour, we must create the file always empty.
    *  If some initial data is present, update after creation with the update function that is idempotent.
    * */

   @Autowired
    lateinit var mongoProp: MongoProperties


    override fun createFile(filename: String, fileContent: InputStream, contentType: String, currentOffset: Long, contentLength: Long) {
        //Using grid we don't have lastOffset and other custom fields, so we must add if we want they for visualization
        //But we have optimal index creation without managing it explicitly
        //The only way to use a custom chunkSize is specifying it each time we store a new file
/*        val emptyContent = ""
        val uploadObject = GridFsUpload
            .fromStream(emptyContent.byteInputStream())
            .filename(filename)
            .contentType(contentType)
            .chunkSize(mongoProp.gridFs.chunkSize)
            .build()
       val savedId = gridFsTemplate.store(uploadObject)*/
        //val savedId = gridFsTemplate.store(emptyContent.byteInputStream(), filename, contentType)

        /**  Using repository and not gridfs**/
                lateinit var savedFile: FileDocument
               try {
                   savedFile = fileRepo.save(FileDocument().apply {
                       this.chunkSize = mongoProp.gridFs.chunkSize.toLong()
                       this.filename = filename
                       this.metadata = org.bson.Document().apply {
                           this["_contentType"] = contentType
                       }
                       this.length = 0//fileContent.size.toLong()
                       this.lastOffset = -1
                   })
               } catch(e:Exception) {
                   throw FileException("Exception saving the file: ${e.message}")
               }

        //val savedFile: FileDocument
        try {
            savedFile = fileRepo.findBy_id(savedFile._id)//savedId)
        } catch(e:Exception) {
            throw FileException("Exception saving the file: ${e.message}")
        }
//        throw FileException("Dead to trigger rollback")
        if(contentLength >0) {
            updateFile(savedFile, fileContent, currentOffset, contentLength)
        }
        else
        /*  No data provided, update only the offset in the root file document. Grid has yet set length to 0
        *   Using repository and not gridfs is possible to anticipate this check and set directly lastOffset creating the document
        * */
            fileRepo.findAndSetLastOffsetBy_id(savedFile._id/*savedId*/, currentOffset)
    }


    override fun updateFile(file: FileDocument, dataToAdd: InputStream, currentOffset: Long, contentLength: Long){
        var numberOfAddedByte = contentLength
        var newNumberOfAddedByte: Long
        try {
            newNumberOfAddedByte = chunkService.appendData(file, file._id, dataToAdd, contentLength)
            if(newNumberOfAddedByte != numberOfAddedByte)
                logger.warn("Number of added byte (stream.available) is different from the one returned by the chunkService")
        }
        catch (e: Exception){
            logger.error { "Update failed, document in files collection has not be modified" }
            throw FileException("Exception updating chunks for the file ${file.filename}: ${e.message}")
        }

        /* Data is updated, now update root file document */
        logger.trace("Updating last offset of the file")
        logger.trace("Updating length of the file")

        /*        val select: Query = Query.query(Criteria.where("_id").`is`(file._id))
               val update = Update()
               update.set("lastOffset", currentOffset)
               update.inc("length", dataToAdd.size.toLong())
               val updateResult : UpdateResult = mongoTemplate.updateFirst(select, update, FileEntity::class.java)
               logger.info("UpdateResult: $updateResult, \n Was ack: ${updateResult.wasAcknowledged()}")*/

        val updateResult = fileRepo.findAndSetLastOffsetAndIncrementLengthBy_id(file._id, currentOffset, newNumberOfAddedByte)
        if(updateResult != 1L)
            throw FileException("Error updating information about ${file.filename}")
    }

    /**********************************
     * Retrieving API
     ********************************/

    override fun getFileEntity(filename: String): FileDocument? {
        var fileDocument: FileDocument? = null
        try {
            //Choose a nullable one... for now this is ok
            fileDocument = fileRepo.findByFilename(filename)
        } catch (e: Exception) {
            logger.error { "Error in retrieving file entity: ${e.message}" }
        }
        return fileDocument
    }

    /**
     * This function imply a further db reading: it could be useful to simplify management of index and offset in mediaStreamLoader
     */
    override fun getFileInfo(filename: String): FileDTO {
        val file =
            try {
                fileRepo.findByFilename(filename)
            }catch (e: Exception){
                throw FileNotFoundException("File with filename: $filename doesn't exist")
            }
        return file.toFileDTO()
    }

    /**
     * Function used only in test. Lead to OOM if file is too big.
     * Use the streaming one instead.
     */
    override fun getEntireResource(filename: String): FileResourceDTO{
       //Must cap this to a max value: OOM problem. Heap saturation if read an entire file.
       //If client doesn't specify blocksize, the entire resource could be too big to read
       val file = fileRepo.findByFilename(filename)
       val chunks = chunkService.getAllChunksOfAFile(file._id)
       val totalData = if(chunks.isEmpty()) byteArrayOf() else chunks.map{ it.data }.reduce{ x, y->x.plus(y)}
       return FileResourceDTO(file = file.toFileDTO(), data = totalData.take(file.length.toInt()).toByteArray())
   }

   @Value("\${reading-block-size}")
   var maximumBlockSize: Int? = null

   override fun getResourceBlock(filename: String, blockSize: Int?, offset: Int?): FileResourceBlockDTO {
       val file =
       try {
           fileRepo.findByFilename(filename)
       }catch (e: Exception){
           throw FileNotFoundException("File with filename: $filename doesn't exist")
       }

       val start = offset ?: 0
       if (start > file.length) {
           return FileResourceBlockDTO(file = file.toFileDTO(), data = byteArrayOf(), offset = start, returnedBlockSize = 0, requestedBlockSize = blockSize ?: 0)
           //Decide if throw an exception or return an empty block
           // throw FileException("Offset is greater than file length")
       }
/*        if(blockSize == null) {
           //Must cap this to a max value: OOM problem. Heap saturation if read an entire file.
           //If client doesn't specify blocksize, the entire resource could be too big to read
           val fileResource = getEntireResource(filename)
           return FileResourceBlockDTO(file = fileResource.file, data = fileResource.data.drop(start).toByteArray(), offset = start, returnedBlockSize = fileResource.data.size-start, requestedBlockSize = blockSize ?: 0)
       }*/
       //println("maximumBlockSize: $maximumBlockSize")
       var readSize = blockSize ?: maximumBlockSize!! //(4 * file.chunkSize.toInt())//(file.length - start).toInt()
       //If different from null, trim it if maximum block size is exceeded
       readSize = Math.min(readSize, maximumBlockSize!!)
       ///select min between readSize and length of file - start
       readSize = Math.min(readSize, (file.length - start).toInt())
/*        readSize = if(blockSize == null)
           4*file.chunkSize.toInt()//(file.length - start).toInt()
       else
           Math.min(blockSize, (file.length - start).toInt())*/
/*        var readSize = blockSize
       ///select min between readSize and length of file - start
       readSize = Math.min(readSize, (file.length - start).toInt())*/
       // Possible optimization: to apply only if the readSize exceed one chunk (considering the start offset).
       // if the readSize exceed 'a bit' the dimension of the last full chunk, then read the entire next chunk too
       // Or read less byte, until the last full chunk
       // Pay attention: start and end are the offset of the specific byte
       // e.g. start 0, readSize 1 => end 0
       // chunkSize, readSize are number of byte, so if the readSize is 1024, the start is 0, the end is 1023
       // but if start == file.length, start-file.length == 0 == readSize, end < start!
       val end = start+readSize-1
       //Take and drop are not a problem (even if value is larger than the list size)
       //I want to change the end, to set returnedBlockSize to the real number of byte read
       val returnedBlockSize = end-start+1

       val chunkList = if(end<start) listOf() else chunkService.getSpecificChunks(file._id, start, end)

       if(chunkList.isEmpty())
           //throw FileException("No chunks found for the requested file")
           //Return an empty fileDTO. Maybe it isn't possible to obtain an empty list with the current implementation.
           return FileResourceBlockDTO(
               file = file.toFileDTO(),
               data = byteArrayOf(),
               offset = start,
               requestedBlockSize = blockSize?:0,
               returnedBlockSize = returnedBlockSize)
/*        val selectedData = chunkList
           .flatMap { it.data.toList() }
           .drop((start%file.chunkSize).toInt())
           .take(returnedBlockSize)
           .toByteArray()*/
       return FileResourceBlockDTO(
           file = file.toFileDTO(),
           data = chunkList
               .flatMap { it.data.asIterable() }
               .toByteArray()
               .copyOfRange((start%file.chunkSize).toInt(), (start%file.chunkSize).toInt()+returnedBlockSize),
           offset = start,
           requestedBlockSize = blockSize?:0,
           returnedBlockSize = returnedBlockSize)
   }

    @Value("\${streaming-buffer-size}")
    val STREAMING_BUFFER_SIZE: Int? = null
    override fun getResourceBlockStreaming(filename: String, startPosChecked: Long, endPosChecked: Long): StreamingResponseBody  {
        //Index are checked in the stream method: so they are valid or equal to 0 (check if this doesn't cause problems)
        val buffer = ByteArray(STREAMING_BUFFER_SIZE ?: (1024 * 1024))
        return StreamingResponseBody { osNonBuff: OutputStream ->
            val os= BufferedOutputStream(osNonBuff)
            val file = fileRepo.findByFilename(filename)
            logger.trace { "I'm in the lambda for a get streaming" }
            logger.trace { "startPosChecked: $startPosChecked" }
            logger.trace { "endPosChecked: $endPosChecked" }
            logger.trace { "filename: ${file.filename}" }
            try {
                //Valutare quel break... può succedere che la lista sia vuota?
                var pos = startPosChecked.toInt()
                logger.info { "pos: $pos" }
                /**
                 * This solution that use a buffer (1MB)
                 *  - With read and n: 19MB 0.07s
                 *  - With readNbytes(STREAMING_BUFFER_SIZE=1MB): 19MB 0.08s
                 */
/*              val retrievedResFile = gridFs.getResource(filename)
                val inputStream= retrievedResFile.content
                //This is called only one time per lambda execution (i.e. the first time, with the first startPosChecked)
                if(pos!=0)
                    inputStream.skip(pos.toLong())
                while (true) {
//                    os.write(inputStream.readNBytes(STREAMING_BUFFER_SIZE))
                    val n = inputStream.read(buffer)

                    if (n == -1)
                        break
                    os.write(buffer, 0, n)
                }
                os.flush()*/
                /**
                 * This solution that not use buffer:
                 * - 19MB  190MB/s  in 0.1s
                 * - 1,59G  70,1MB/s    in 22s
                 * - 1,04G  75,7MB/s    in 14s
                 * - 563,13M  74,2MB/s    in 7,6s
                 */
/*                val retrievedResFile = gridFs.getResource(filename)
                val inputStream= retrievedResFile.content
                //This is called only one time per lambda execution (i.e. the first time, with the first startPosChecked)
                if(pos!=0)
                    inputStream.skip(pos.toLong())
                while (true) {
                    val n = inputStream.transferTo(os)

                    if(n.toInt() <=0)
                        break
                }
                os.flush()*/
                /**
                 * This solution that use custom API :
                 *  -  19,16M  16,8MB/s    in 1,1s
                 *  - 1,59G  17,0MB/s    in 1m 46s
                 *  - 1,04G  15,6MB/s    in 67s
                 *  - 563,13M  15,7MB/s    in 36s
                 * With sequence:
                 *  - 1,59G  14,9MB/s    in 1m 45s
                 *  - 1,04G  15,1MB/s    in 67s
                 *  - 563,13M  15,1MB/s    in 36s
                 *
                 */
                //Usando la versione sequence (ma forse anche nell'altra) c'è un problema con l'ultimo byte. Sembra come se la fileEndPos di là debba essere incrementata di uno, oppure di qua il ciclo sia <=. O endPosCheked +1
                while (pos < endPosChecked) {
                    val endValue = (pos + buffer.size).coerceAtMost((endPosChecked+1).toInt())
                    val chunkList = chunkService.getSpecificChunks(file._id, pos, endValue)/**!!  +1   !!*/
                    if (chunkList.isEmpty())
                        break
                    val relativeStart = pos % file.chunkSize.toInt()
                  /*  chunkList
                        .asSequence()
                        .flatMap { it.data.asIterable() }
                        .drop(relativeStart)
                        .take(buffer.size) //It's not necessary coerce at most as with a copyOfRange
                        .forEach { os.write(byteArrayOf(it)) }*/
                    //Devo stabilire qual è la lunghezza del bytearray letto, per calcolare gli estremi del copyRange
                    //il range sarà grande buffer.size, tranne nel caso in cui abbia letto di meno (il contenuto era terminato)
                    //Questa è al massimo buffer.size
                    var lengthOfContentRead =0
                    //val relativeEnd = if((pos+buffer.size)>endPosChecked.toInt()) (relativeStart+(endPosChecked-pos)) else (relativeStart+buffer.size)//relativeStart + (endValue - pos)
                    //Due alternative. Provare a usare l'aggregation per restituire solo i bytearray
                    //Provare a cambiare il sequence di prima
                    var totalNumberOfData =0
                    val totalNumberOfChunk = chunkList.size
                    //This is lastOffset of last chunk retrieved
                    val relativeEnd = endValue % file.chunkSize.toInt()
                    //Nel for each dovrò capire quando sono nell'ultimo chunk
                    /** 1,59G  34,9MB/s    in 37s con picchi sui 50 MB/s. Problema della chiusura (get range con posizione uguale alla dimensione del file) da gestire
                     */
                    try {
                        chunkList
                            .asSequence()
                            .map { it.data }
                            .forEachIndexed { index, bytes -> run {
                                when (index){
                                    //First
                                    0 -> {
                                        //and single chunk
                                        totalNumberOfData += if (totalNumberOfChunk == 1) {
                                            os.write(bytes.copyOfRange(relativeStart, relativeEnd))
                                            relativeEnd - relativeStart
                                        } else {
                                            os.write(bytes.copyOfRange(relativeStart, bytes.size))
                                            bytes.size - relativeStart
                                        }
                                    }
                                    //Last chunk
                                    totalNumberOfChunk - 1 -> {
                                        os.write(bytes.copyOfRange(0, relativeEnd))
                                        totalNumberOfData += relativeEnd
                                    }
                                    //Middle chunk
                                    else -> {
                                        os.write(bytes)
                                        totalNumberOfData += bytes.size
                                    }
                                }
                            }
                            }

                    }
                    catch (e: Exception) {
                        break
                    }
/*                    buffer = chunkList
                        .flatMap { it.data.asIterable() }
                        .toByteArray()
                        .apply { lengthOfContentRead = this.size }
                        .copyOfRange(relativeStart, (relativeStart + buffer.size).coerceAtMost(lengthOfContentRead) )
                    try {
                        //The underlying stream may not know it its closed until you attempt to write to it (e.g. if the other end of a socket closes it)
                        //The simplest approach is to use it and handle what happens if it closed then, rather than testing it first as well.
                        // https://stackoverflow.com/questions/8655901/how-to-check-whether-an-outputstream-is-closed
                        //Questa soluzione dà problemi di riproduzione con file grandi.
                        // Lo stream è spesso chiuso... e si impalla
                        os.write(buffer)
                    } catch (e: Exception) {
                        logger.warn { "I/O Exception in writing to outputstream. It is: ${e.message}" }
                        //Break is right? Otherwise playing big file too many resource remain writing on a closed buffer
                        break
                        //throw e
                    }*/
                    pos = endValue
                }
                os.flush()
            }
            catch (e: Exception) {
                logger.error { "Error in streaming file: ${e.message}" }
                throw e
            }
        }
    }

    /**
     * Old version. Deprecated and considered not useful. If necessary, update it before
     */
    val EXCEEDING_RATE = 0.0 // 0.1
    override fun getResourceBlockOptimizedVersion(filename: String, blockSize: Int?, offset: Int?): FileResourceBlockDTO {
       val file =
           try {
               fileRepo.findByFilename(filename)
           }catch (e: Exception){
               throw FileNotFoundException("File with filename: $filename doesn't exist")
           }

       val start = offset ?: 0
       if (start > file.length) {
           return FileResourceBlockDTO(file = file.toFileDTO(), data = byteArrayOf(), offset = start, returnedBlockSize = 0, requestedBlockSize = blockSize ?: 0)
           //Decide if throw an exception or return an empty block
           // throw FileException("Offset is greater than file length")
       }
       // Chose a proper default value for blockSize
       var readSize = blockSize ?: file.chunkSize.toInt()
       ///select min between readSize and length of file - start
       readSize = Math.min(readSize, (file.length - start).toInt())
       // Possible optimization: to apply only if the readSize exceed one chunk (considering the start offset).
       // if the readSize exceed 'a bit' the dimension of the last full chunk, then read the entire next chunk too
       // Or read less byte, until the last full chunk
       // Pay attention: start and end are the offset of the specific byte
       // e.g. start 0, readSize 1 => end 0
       // chunkSize, readSize are number of byte, so if the readSize is 1024, the start is 0, the end is 1023
       val end : Int
       if(readSize+(start%(file.chunkSize)) > file.chunkSize) {
           // Index of the byte in the last chunk
           val exceedingByte = (start+readSize) % file.chunkSize.toInt()
           //val relativeEndOffset = exceedingByte-1
           // Evaluate, choosing a proper metrics, if I must read the next chunk too
           // relativeEndOffset + 1 is the number of exceeding byte
           if (exceedingByte >= file.chunkSize*EXCEEDING_RATE) {
               // Read the next chunk too
               end = start+readSize-1
           }
           else {
               // Ignore the last bytes of the readBlock
               //val relativeIndex = start%file.chunkSize
               //val remaining = file.chunkSize-relativeIndex
               //end = start+remaining-1
               end = start+readSize-exceedingByte-1
           }
       }
       else{
           // Read the requested bytes because doesn't exceed the size of a chunk
           end = start+readSize-1
       }
       //Take and drop are not a problem (even if value is larger than the list size)
       //I want to change the end, to set returnedBlockSize to the real number of byte read

       val returnedBlockSize = end-start+1

       val chunkList = chunkService.getSpecificChunks(file._id, start, end)
       if(chunkList.isEmpty())
       //throw FileException("No chunks found for the requested file")
       //Return an empty fileDTO. Maybe it isn't possible to obtain an empty list with the current implementation.
           return FileResourceBlockDTO(
               file = file.toFileDTO(),
               data = byteArrayOf(),
               offset = start,
               requestedBlockSize = blockSize?:0,
               returnedBlockSize = returnedBlockSize)
       val selectedData = chunkList
           .flatMap { it.data.toList() }
           .drop((start%file.chunkSize).toInt())
           .take(returnedBlockSize)
           .toByteArray()
//        chunks.map { it.data }.reduce{x, y->x.plus(y)}.drop((start%file.chunkSize).toInt()).take(returnedBlockSize).toByteArray()
//        chunks.map { it.data }.reduce{x,y->x.plus(y)}.slice((start%file.chunkSize).toInt() until (end%file.chunkSize+1).toInt()).toByteArray()
       //Take and drop are not a problem (even if value is larger than the list size)
       //Using they as offset could be a problem
       return FileResourceBlockDTO(
           file = file.toFileDTO(),
           data = selectedData,
           offset = start,
           requestedBlockSize = blockSize?:0,
           returnedBlockSize = returnedBlockSize)
   }

   //logDbStats works only with a single instance of StorageService

   lateinit var resultStats: Document
   lateinit var fileCollectionStats: Document
   lateinit var chunksCollectionStats: Document

   fun logDbStats(message: String){
       //switch if message is initStats log stats
       when(message) {
           "initStats" -> {
               resultStats = this.mongoTemplate.db.runCommand(Document(mapOf("dbstats" to 1, "scale" to 1048576)))
               fileCollectionStats = this.mongoTemplate.db.runCommand(Document("collStats", "rcs.files"))
               chunksCollectionStats = this.mongoTemplate.db.runCommand(Document("collStats", "rcs.chunks"))
               logger.info { "**** BEFORE ****" }
               //logger.info { "DB stats in MB: $resultStats" }
               logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStats["fsUsedSize"] as Double}" }
               //logger.info { "File collection stats: $fileCollectionStats" }
               //logger.info { "Chunks collection stats: $chunksCollectionStats" }
               logger.info { "The total size of all indexes, with scale Bytes" }
               logger.info { "File collection totalIndexSize: ${fileCollectionStats["totalIndexSize"]}" }
               logger.info { "Chunks collection totalIndexSize: ${chunksCollectionStats["totalIndexSize"]}" }
/*                val stats = mongoTemplate.executeCommand("{dbstats: 1}")
               logger.info("DB stats: $stats")*/
           }
           "endStats" ->{
               val resultStatsAfter= this.mongoTemplate.db.runCommand(Document(mapOf("dbstats" to 1, "scale" to 1048576)))
               val fileCollectionStatsAfter= this.mongoTemplate.db.runCommand(Document("collStats", "rcs.files"))
               val chunksCollectionStatsAfter= this.mongoTemplate.db.runCommand(Document("collStats", "rcs.chunks"))
               logger.info { "**** AFTER ****"}
               //logger.info { "DB stats: $resultStatsAfter" }
               logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStatsAfter["fsUsedSize"] as Double}" }
               //logger.info { "File collection stats: $fileCollectionStatsAfter" }
               //logger.info { "Chunks collection stats: $chunksCollectionStatsAfter" }
               logger.info { "The total size of all indexes, with scale Bytes" }
               logger.info { "File collection totalIndexSize: ${fileCollectionStatsAfter["totalIndexSize"]}"}
               logger.info { "Chunks collection totalIndexSize: ${chunksCollectionStatsAfter["totalIndexSize"]}"}

               logger.info { "**** DIFFERENCE ****"}
               logger.info { "FileSystem used size for the entire db, with scale=1048576 (i.e. 1 MBi): ${resultStatsAfter["fsUsedSize"] as Double - resultStats["fsUsedSize"] as Double}" }
               logger.info { "The total uncompressed size in memory of all records in a collection. The size does not include the size of any indexes associated with the collection, which the totalIndexSize field reports."}
               logger.info { "File collection size difference: ${fileCollectionStatsAfter["size"] as Int - fileCollectionStats["size"] as Int}" }
               logger.info { "Chunks collection size difference: ${chunksCollectionStatsAfter["size"] as Int - chunksCollectionStats["size"] as Int}" }
               logger.info { "File collection totalIndexSize difference: ${fileCollectionStatsAfter["totalIndexSize"] as Int - fileCollectionStats["totalIndexSize"] as Int}" }
               logger.info { "Chunks collection totalIndexSize difference: ${chunksCollectionStatsAfter["totalIndexSize"] as Int - chunksCollectionStats["totalIndexSize"] as Int}" }
           }

           else -> logger.info("Invalid message to log stats: $message")

       }
   }

    /*  val fos = FileOutputStream("fileDiTest", true)
    var size =0L
    var timeBefore = 0L
    @KafkaListener(
        id = "unique",
        topics = ["fileStorage"],
        autoStartup = "false",
        idIsGroup = false*//*, groupId = "group_id_rcs"*//*
    )
    fun consumerForTest(consumerRecord: ConsumerRecord<Int, KafkaPayloadDTO>, ack: Acknowledgment){
        if(size==0L)
            timeBefore= System.currentTimeMillis()
        //Just consume all the message in the topic
        val fileContent = consumerRecord.value().dataToStoreDTO.fileContent
        //fos.write(fileContent)
        size+= fileContent.size
        if(size == MEBIBYTE*1024){
            val timeAfter = System.currentTimeMillis()
            //fos.flush()
//            fos.close()
            println("Time to pull all the topic: ${timeAfter-timeBefore}")
            gridFsTemplate.store(byteArrayOf(2).inputStream(), "finito${(timeBefore-timeAfter)/1000}")
        }
    }
    */

    //Convert a string with space in a string comma separated
    fun String.toCsvString(): String {
        return this.split(" ").joinToString(separator = ",")
    }

    //function to save to a file a list of string through a csv print writer
    @Synchronized
    fun MutableList<String>.saveToCsvFile(filePath:String, fileName: String) {
        //current path of work dir
        logger.info { "Path of current workdir: ${System.getProperty("user.dir")}" }
        val newPath= System.getProperty("user.dir")+filePath+"/"+fileName
        logger.info {"File path: $newPath"}
        Files.write(
            Paths.get(newPath), this.map { it.toCsvString() }, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }


    @Value("\${spring.kafka.consumer.max-partition-fetch-bytes}")
    lateinit var maxPartitionFetchBytes: String
    @Value("\${spring.kafka.concurrency-listener}")
    lateinit var concurrencyListener: String
    @Value("\${spring.kafka.topics.number-of-partitions}")
    lateinit var numberOfPartitions: String

    //Information needed to save the stats for benchamarking
    @Value("\${number-of-generator-threads}")
    var numberOfGeneratorThreads: Int = 0
    @Value("\${sending-bytes-rate}")
    var sendingBytesRate: Int = -1

    @Value("\${kind-of-transactionality}")
    lateinit var kot: String
    val commonStatsFile: String by lazy {
        //switch on maxPartitionFetchBytes
        val mpfb = when (maxPartitionFetchBytes) {
            "1048576" -> "1"
            "5242880" -> "5"
            "10485760" -> "10"
            "20971520" -> "20"
            else -> "unknownMaxPartitionFetchBytes"
        }
        val familyCurve = when (sendingBytesRate){
            1 -> "a"
            5 -> "b"
            50 -> "c"
            -1 -> ""
            else -> "unknownRate"
        }

        //val csf="bench${kot}_${System.currentTimeMillis().mod(1000)}.csv"
        //To maintain compatibility with the old version of benchmarking
        val csf= if(sendingBytesRate==-1)
            ".env.$numberOfGeneratorThreads$numberOfPartitions${mpfb}${kot.uppercase()}.csv"
        else
            ".env.$familyCurve$numberOfGeneratorThreads$sendingBytesRate$numberOfPartitions${mpfb}${kot.uppercase()}.csv"
        logger.info { "Common stats file: $csf" }
        val dataLines: MutableList<String> = mutableListOf()
        dataLines.add("ConcurrencyListener NumberOfPartitions MaxPartitionFetchBytes SendingBytesRate NumberOfGeneratorThreads")
        dataLines.add("$concurrencyListener $numberOfPartitions $maxPartitionFetchBytes $sendingBytesRate Mbi/s $numberOfGeneratorThreads")
        dataLines.add("Filename (ms)TimeToPersistLastMessage")
        try {
            //with /data we have absolute path. Only data/stats is relative to workspace
            dataLines.saveToCsvFile("/data/stats", csf)
        } catch (e: Exception) {
            logger.error("Error while saving stats: $e")
        }
        csf
    }

    val commonBenchDataFile: String by lazy {
        //switch on maxPartitionFetchBytes
        val mpfb = when (maxPartitionFetchBytes) {
            "1048576" -> "1"
            "5242880" -> "5"
            "10485760" -> "10"
            "20971520" -> "20"
            else -> "unknownMaxPartitionFetchBytes"
        }
        val familyCurve = when (sendingBytesRate){
            1 -> "a"
            5 -> "b"
            50 -> "c"
            8 -> "c"
            -1 -> ""
            else -> "unknownRate"
        }

        //val csf="bench${kot}_${System.currentTimeMillis().mod(1000)}.csv"
        //To maintain compatibility with the old version of benchmarking
        val csf= if(sendingBytesRate==-1)
            ".env.$numberOfGeneratorThreads$numberOfPartitions${mpfb}${kot.uppercase()}.csv"
        else
            ".env.$familyCurve$numberOfGeneratorThreads$sendingBytesRate$numberOfPartitions${mpfb}${kot.uppercase()}.csv"
        logger.info { "Common stats file: $csf" }

        val dataLines: MutableList<String> = mutableListOf()
        dataLines.add("ConcurrencyListener NumberOfPartitions MaxPartitionFetchBytes SendingBytesRate NumberOfGeneratorThreads")
        dataLines.add("$concurrencyListener $numberOfPartitions $maxPartitionFetchBytes ${sendingBytesRate}Mbi/s $numberOfGeneratorThreads")
        dataLines.add("Filename MessageOffset SendingTime ReceivingTime SavingTime SavedSize Latency(ms) RelativeLatency(BaseTime)(ms) OriginalFilename AssignedPartition")
        try {
            //with /data we have absolute path. Only data/stats is relative to workspace
            dataLines.saveToCsvFile("/data/stats", csf)
        } catch (e: Exception) {
            logger.error("Error while saving stats: $e")
        }
        csf
    }
    private fun logTime(baseTime: Long, filename: String) {
        val time = System.currentTimeMillis() - baseTime
        logger.info("Last message for file: $filename processed and committed at: $time ms")
        val dataLines: MutableList<String> = mutableListOf()
        //dataLines.add("Filename TimeToConsumeAndPersistAllTheMessageForTheFile(ms)Relative")
        dataLines.add("$filename ${time}")
        try {
            //with /data we have absolute path. Only data/stats is relative to workspace
            dataLines.saveToCsvFile("/data/stats", commonStatsFile)
        } catch (e: Exception) {
            logger.error("Error while saving stats: $e")
        }
    }

    private fun logTimeForFile(benchmarkingData: MutableList<BenchmarkingData>){
        logger.info("Last message for file: ${benchmarkingData.last().filename} processed and committed at: ${benchmarkingData.last().relativeLatency} ms")
        val dataLines: MutableList<String> = mutableListOf()
        //dataLines.add("Filename TimeToConsumeAndPersistAllTheMessageForTheFile(ms)Relative")

        val actualFile = dumpedFile.incrementAndGet()
        val incrementalNameToUse = if(actualFile<10) "0${actualFile}" else "$actualFile"
        benchmarkingData.forEach{dataLines.add("File$incrementalNameToUse ${it.messageOffset} ${it.sendingInstant} ${it.receivedInstant} ${it.savingInstant} ${it.savedSize} ${it.latency} ${it.relativeLatency} ${it.filename} ${it.assignedPartition}")}
        try {
            //with /data we have absolute path. Without starting slash data/stats is relative to workspace
            dataLines.saveToCsvFile("/data/stats", commonBenchDataFile)
        } catch (e: Exception) {
            logger.error("Error while saving stats: $e")
        }
        if(dumpedFile.get() >= numberOfGeneratorThreads){
            val lastLine = mutableListOf("END")
            lastLine.saveToCsvFile("/data/stats", commonBenchDataFile)
        }

    }

}