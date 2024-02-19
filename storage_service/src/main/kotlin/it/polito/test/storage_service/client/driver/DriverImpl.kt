package it.polito.test.storage_service.client.driver

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import java.io.ByteArrayInputStream

class DriverImpl: Driver {
    override fun writeOnDisk(kafkaTemplate: KafkaTemplate<String, Message<Any>>, topic: String, filename: String, data: ByteArray, metadata: MutableMap<String, String>, message: String) {
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"]
        //a bytearray of 1048576 result in a message of 1398370
        //With a difference of 349794 (341 KB)... so we need to take at least 400KB of data less
        val maxDataDimension = maxRequestSize as Int - (400*1024)
        var singleMessage = true
//        var bytesToSend: List<Byte> = data.asList()

        var counter: Int = 0
        while ((data.size-counter) > maxDataDimension){
            singleMessage = false
            //val bytesForMessage = bytesToSend.take(maxDataDimension)
            //Not necessary. With this I can remove the second part
            val nBytesForMessage = minOf(data.size-counter, maxDataDimension)
            val dataToStoreDTO = DataToStoreDTO(filename, data.copyOfRange(counter, counter + nBytesForMessage), metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.MESSAGE_KEY, filename)
                .build()
            kafkaTemplate.send(messageForKafka)
            counter += nBytesForMessage
        }
        if(singleMessage || counter != data.size){
            val dataToStoreDTO = DataToStoreDTO(filename, data.copyOfRange(counter, data.size), metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.MESSAGE_KEY, filename)
                .build()
            kafkaTemplate.send(messageForKafka)
        }
    }

    override fun writeOnDiskByteInputStream(kafkaTemplate: KafkaTemplate<String, Message<Any>>, topic: String, filename: String, data: ByteArrayInputStream, metadata: MutableMap<String, String>, message: String) {
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"]
        //a bytearray of 1048576 result in a message of 1398370
        //With a difference of 349794 (341 KB)... so we need to take at least 400KB of data less
        val maxDataDimension = maxRequestSize as Int - (400*1024)
        var singleMessage = true
//        var bytesToSend: List<Byte> = data.asList()

        var counter: Int = 0
        while ((data.available()-counter) > maxDataDimension){
            singleMessage = false
            //val bytesForMessage = bytesToSend.take(maxDataDimension)
            //Not necessary. With this I can remove the second part
            val nBytesForMessage = minOf(data.available()-counter, maxDataDimension)
            val dataToStoreDTO = DataToStoreDTO(filename, data.skip(counter.toLong()).let { data.readNBytes(nBytesForMessage) }, metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.MESSAGE_KEY, filename)
                .build()
            kafkaTemplate.send(messageForKafka)
            counter += nBytesForMessage
        }
        if(singleMessage || counter != data.available()){
            val dataToStoreDTO = DataToStoreDTO(filename, data.skip(counter.toLong()).let { data.readAllBytes() }, metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.MESSAGE_KEY, filename)
                .build()
            kafkaTemplate.send(messageForKafka)
        }
    }

    /*override fun writeOnDisk(kafkaTemplate: KafkaTemplate<String, Message<Any>>, topic: String, filename: String, data: ByteArray, metadata: MutableMap<String, String>, message: String) {
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"]
        //a bytearray of 1048576 result in a message of 1398370
        //With a difference of 349794 (341 KB)... so we need to take at least 400KB of data less
        val maxDataDimension = maxRequestSize as Int - (400*1024)
        var singleMessage = true
//        var bytesToSend: List<Byte> = data.asList()
        var bytesToSend = data
        while (bytesToSend.size > maxDataDimension){
            singleMessage = false
            val bytesForMessage = bytesToSend.take(maxDataDimension)
            bytesToSend = bytesToSend.drop(maxDataDimension).toByteArray()
            val dataToStoreDTO = DataToStoreDTO(filename, bytesForMessage.toByteArray(), metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()
            kafkaTemplate.send(messageForKafka)
        }
        if(singleMessage || bytesToSend.isNotEmpty()) {
            val dataToStoreDTO = DataToStoreDTO(filename, bytesToSend, metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()
            kafkaTemplate.send(messageForKafka)
        }
    }*/

    override fun writeOnDiskWithStats(kafkaTemplate: KafkaTemplate<String, Message<Any>>, topic: String, filename: String, data: ByteArray, metadata: MutableMap<String, String>) {
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"]
        //a bytearray of 1048576 result in a message of 1398370
        //With a difference of 349794 (341 KB)... so we need to take at least 400KB of data less
        val maxDataDimension = maxRequestSize as Int - (400*1024)
        var singleMessage = true
        var bytesToSend = data
        while (bytesToSend.size > maxDataDimension){
            //If singleMessage is false, it means that the message is not the first one
            val message = if (singleMessage) "initStats" else "File must be saved on disk"
            singleMessage = false
            val bytesForMessage = bytesToSend.take(maxDataDimension)
            bytesToSend = bytesToSend.drop(maxDataDimension).toByteArray()
            val dataToStoreDTO = DataToStoreDTO(filename, bytesForMessage.toByteArray(), metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()
            kafkaTemplate.send(messageForKafka)
        }
        if(singleMessage || bytesToSend.isNotEmpty()) {
            val message = if(singleMessage) "logStats" else "endStats"
            val dataToStoreDTO = DataToStoreDTO(filename, bytesToSend, metadata)
            val kafkaPayloadDTO = KafkaPayloadDTO(dataToStoreDTO, message)
            val messageForKafka: Message<KafkaPayloadDTO> = MessageBuilder
                .withPayload(kafkaPayloadDTO)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()
            kafkaTemplate.send(messageForKafka)
        }
    }
}