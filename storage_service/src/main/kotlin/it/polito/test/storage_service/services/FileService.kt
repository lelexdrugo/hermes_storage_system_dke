package it.polito.test.storage_service.services

import it.polito.test.storage_service.dtos.FileDTO
import it.polito.test.storage_service.dtos.FileResourceBlockDTO
import it.polito.test.storage_service.dtos.FileResourceDTO
import it.polito.test.storage_service.documents.FileDocument
import it.polito.test.storage_service.dtos.KafkaPayloadDTO
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.InputStream

interface FileService {
    /**
     * Create a new file in MongoDB.
     * The method is responsible to create a new document in .files collection and to call updateFile to add data for that file if fileContent is not empty.
     * This guarantee idempotent behaviour consuming message even in case of failure.
     * Root document for the file is created using store by GridFTemplate with empty content.
     * In this all the needed index are created if absent.
     * @param filename Name of the file
     * @param fileContent The bytes constituting the file
     * @param contentType Content of the media (e.g. "document/docx")
     * @param currentOffset Offset of the message that create the file in the related kafka topic partition. Default value is -1, even if is created by rest endpoint
     */
    fun createFile(filename: String, fileContent: InputStream, contentType: String, currentOffset: Long, contentLength: Long)

    /**
     * Update a file in MongoDB, writing data with append option.
     * The method is responsible to update the document passed as parameter, creating all the needed chunks. This is possible using method from ChunkService.
     * It is also responsible to update the root document for the file, updating the length and the lastOffset property.
     * @param file The file to update
     * @param dataToAdd The bytes to add to the file
     * @param currentOffset Offset of the message that contains the data in the related kafka topic partition.
     * @param contentLength Total length of the data to append.
     */
    fun updateFile(file: FileDocument, dataToAdd: InputStream, currentOffset: Long, contentLength: Long)

    /**
     * Retrieve a block of a file
     * @param filename Name of the file
     * @param blockSize Size of the block to retrieve. If null, service use the value contained in application.properties as maxBlockSize
     * @param offset Offset of the byte from which start the block. If null, service read from the beginning
     * @return: a DTO containing file metadata, the block of data, the starting offset and the number of read byte
     */
    fun getResourceBlock(filename: String, blockSize: Int?, offset: Int?) : FileResourceBlockDTO

    /**
     * Create an object implementing StreamingResponseBody functional interface, to stream the content of a file with asynchronous request processing.
     * The application can write directly to the response OutputStream without holding up the Servlet container thread.
     * The method retrieve requested bytes from the file using method from ChunkService, so in the form of chunkList.
     * It extracts content from that list and made it available to write to the response OutputStream.
     * @param filename Name of the file
     * @param startPosChecked Starting position of the block to retrieve
     * @param endPosChecked Ending position of the block to retrieve
     */
    fun getResourceBlockStreaming(filename: String, startPosChecked: Long, endPosChecked: Long): StreamingResponseBody

    /**
     * Retrieve a FileDTO object through its name
     * @param filename Name of the file
     * @throws FileNotFoundException if such file doesn't exist
     */
    fun getFileInfo(filename: String): FileDTO

    /**
     * !UNUSABLE! Old version. Need to realign to getResourceBlock, but maybe it has no more meaning.
     * Retrieve a block of a file
     * @param filename Name of the file
     * @param blockSize Size of the block to retrieve. If null, service choose a proper value
     * @param offset Offset of the byte from which start the block. If null, service read from the beginning
     * @return: a DTO containing file metadata, the block of data, the starting offset and the number of read byte
     */
    fun getResourceBlockOptimizedVersion(filename: String, blockSize: Int?, offset: Int?) : FileResourceBlockDTO

    /**
     * Listener method for Kafka consumer.
     * Process a message from a Kafka topic, creating or updating a file in MongoDB.
     * Recognize if a message was already process and skip it.
     * It uses method createFile and updateFile to do that.
     * If everything was fine it acknowledge the message, so the offset is updated and the message is not processed again.
     * @param consumerRecord The message to process
     * @param ack The Acknowledgment object to acknowledge the message
     */
    fun singleMessageConsumer(consumerRecord: ConsumerRecord<String, KafkaPayloadDTO>, ack: Acknowledgment)

    /**
     * Listener method for Kafka consumer supporting batch.
     * Simply process each message from the collection, creating or updating a file in MongoDB.
     * Recognize if a message was already process and skip it.
     * It uses method createFile and updateFile to do that.
     * If a fail occurs, it partially acknowledges the message, so only the offset of processed messages is updated and they are not processed again.
     * If everything was fine it acknowledge the batch, so the offset of all the message are committed, and they are not processed again.
     * @param listConsumerRecord The list of message to process
     * @param ack The Acknowledgment object to acknowledge the message
     */
    fun batchConsumerSimplyForward(listConsumerRecord: List<ConsumerRecord<String, KafkaPayloadDTO>>, ack: Acknowledgment)

    /**
     * Listener method for Kafka consumer supporting batch.
     * It performs aggregation of content of the messages that update the same file.
     * So it process a batch of messages arriving from the same partition a single message, creating or updating a file in MongoDB.
     * It uses method createFile and updateFile to do that.
     * If everything was fine it acknowledge the batch, so the offset of all the message are committed, and they are not processed again.
     * @param listConsumerRecord The message to process
     * @param ack The Acknowledgment object to acknowledge the message
     */
    fun batchConsumerWithAggregation(listConsumerRecord: List<ConsumerRecord<String, KafkaPayloadDTO>>, ack: Acknowledgment)

    /*
     * Used in test or debug function
     * Could lead to OOM error
     */
    fun getEntireResource(filename: String) : FileResourceDTO
    fun getFileEntity(filename: String) : FileDocument?
}
