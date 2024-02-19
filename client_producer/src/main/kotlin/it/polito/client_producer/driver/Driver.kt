package it.polito.test.storage_service.client.driver

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * Interface for a driver class that allow clients to persist data.
 * The data layer is fed by Hermes microservice, and clients communicate with it pushing message to Kafka.
 * So this driver is in charge of produce message compatible with Hermes.
 * Correspond to the write API of a filesystem
 * Ideally an open api could be provided to obtain a Kafka template object in case client doesn't have one yet
 */
interface Driver {
    /**
     * Method to persist information. Client could use more than one time this function for the same file,
     * obtaining a behaviour similar to write with append option in a filesystem
     * @param kafkaTemplate: An object of class KafkaTemplate, containing producer configuration.
     * @param topic: The name of the topic where the message will be sent
     * @param filename: The name of the file to be saved/updated
     * @param data: The content to be saved
     * @param metadata: A map containing metadata to be saved along with the file
     * @param message: An optional message to be sent inside kafka message
     */
    fun writeOnDisk(kafkaTemplate: KafkaTemplate<String, Message<Any>>, topic: String, filename: String, data: ByteBuffer, metadata: MutableMap<String, String>, message: String = "File must be saved on disk")

    /**
     * Method to persist information. It supports content in ByteArrayInputStream. Could be possible to change it to manage InputStream if useful
     */
    fun writeOnDiskByteInputStream(kafkaTemplate: KafkaTemplate<String, Message<Any>>, topic: String, filename: String, data: ByteArrayInputStream, metadata: MutableMap<String, String>, message: String = "File must be saved on disk")

    /**
     * Debug method used to persist information printing database size statistic
     */
    fun writeOnDiskWithStats(kafkaTemplate: KafkaTemplate<String, Message<Any>>, topic: String, filename: String, data: ByteBuffer, metadata: MutableMap<String, String>)
}