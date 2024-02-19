package it.polito.client_producer.configurations

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "spring.kafka") // to access properties in application.yml file
data class KafkaProperties(
    var bootstrapServers: String = "localhost:29092",
    var topics: TopicsProperties = TopicsProperties(),
    var producer: Producer = Producer()
){
    data class TopicsProperties(
        var listOfTopics: List<String> = listOf("fileStorage", "chunk"),
        var numberOfPartitions: Int = 10,
        var replicationFactor: Short = 1,
        var produce: String = "",
        var consume: String = ""
    )
    class Producer(
        var keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
        var valueSerializer: String = "org.springframework.kafka.support.serializer.JsonSerializer",
        var bufferMemory: Long = 33554432,
        var acks: String = "all",
        var isProducerPerThread: Boolean = true,
        var compressionType: String = "none",
        var linger: Int =0,
        var batchSize: Int = 0
    )
}

