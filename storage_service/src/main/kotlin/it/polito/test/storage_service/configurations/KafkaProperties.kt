package it.polito.test.storage_service.configurations

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "spring.kafka") // to access properties in application.yml file
data class KafkaProperties(
    var bootstrapServers: String = "localhost:29092",
    var topics: TopicsProperties = TopicsProperties(),
    var consumer: Consumer = Consumer(),
    var producer: Producer = Producer(),
    var concurrencyListener: Int = 10,//10
    var batchListener: BatchListenerProperties = BatchListenerProperties()
){
    data class TopicsProperties(
        var listOfTopics: List<String> = listOf("fileStorage"/*, "chunk"*/),
        var numberOfPartitions: Int = 10,//10,
        var replicationFactor: Short = 1,
        var maxMessageBytes: Int = 1048576
    )
    data class Consumer(
        var groupId: String = "group_id_default",
        var autoOffsetReset: String = "earliest",
        var enableAutoCommit: Boolean = false,
        var keyDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
        var valueDeserializer: String = "org.springframework.kafka.support.serializer.JsonDeserializer",
        var trustedPackage: String = "it.rcs.test.storageService.dtos",
        var maxPoolRecords: Int = 500,
        var fetchMaxBytes: Int = 52428800,
        var maxPartitionFetchBytes: Int = 1048576,
    )
    data class Producer(
        var keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
        var valueSerializer: String = "org.springframework.kafka.support.serializer.JsonSerializer",
        var maxRequestSize: Int = 1048576
    )
    data class BatchListenerProperties(
        var enabled: Boolean = false,
        var aggregation: Boolean = true,
    )
}

