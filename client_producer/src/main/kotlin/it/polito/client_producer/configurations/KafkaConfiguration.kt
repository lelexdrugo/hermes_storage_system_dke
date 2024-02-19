package it.polito.client_producer.configurations

import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.*
import org.springframework.messaging.Message

//https://kafka.apache.org/documentation

/**
 * Producer configuration
 */
@Configuration
class KafkaProducerConfig(private val kafkaProp: KafkaProperties) {
        @Bean
        fun producerFactory(): ProducerFactory<String, Message<Any>> {
            val configProps: MutableMap<String, Any> = HashMap()
            configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProp.bootstrapServers
            configProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = kafkaProp.producer.keySerializer
            configProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = kafkaProp.producer.valueSerializer
            //RecordTooLargeException
            configProps[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = 1048576
            //Default:	33554432. This value can cause OOM error in the producer.
            //I've to change to more than 100MB to correctly simulate the upload of a file with a 100MB/s speed
            configProps[ProducerConfig.BUFFER_MEMORY_CONFIG] = kafkaProp.producer.bufferMemory
            configProps[ProducerConfig.ACKS_CONFIG] = kafkaProp.producer.acks
            configProps[ProducerConfig.COMPRESSION_TYPE_CONFIG] = kafkaProp.producer.compressionType
            configProps[ProducerConfig.COMPRESSION_TYPE_CONFIG] = kafkaProp.producer.compressionType
            configProps[ProducerConfig.LINGER_MS_CONFIG] = kafkaProp.producer.linger
            configProps[ProducerConfig.BATCH_SIZE_CONFIG] = kafkaProp.producer.batchSize
            //Default: 60000
//            configProps[ProducerConfig.MAX_BLOCK_MS_CONFIG] = 1200000
            val dpf = DefaultKafkaProducerFactory<String,Message<Any>>(configProps)
            //https://stackoverflow.com/questions/64389445/proceeding-to-force-close-the-producer-since-pending-requests-could-not-be-compl
            dpf.setPhysicalCloseTimeout(300)
            dpf.isProducerPerThread = kafkaProp.producer.isProducerPerThread
            dpf.isProducerPerConsumerPartition = false
            return dpf
        }

        @Bean
        fun kafkaTemplate(): KafkaTemplate<String, Message<Any>> {
            return KafkaTemplate(producerFactory())
        }
}

/**
 * Topic configuration
 */
@Configuration
class KafkaTopicConfig(private val kafkaProp: KafkaProperties) {

    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs: MutableMap<String, Any?> = HashMap()
        configs[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProp.bootstrapServers
        return KafkaAdmin(configs)
    }
    @Bean
    fun topics(): KafkaAdmin.NewTopics {
        val props: MutableMap<String, String> = HashMap()
        //Default:	1048588
        props[TopicConfig.MAX_MESSAGE_BYTES_CONFIG] = "1048576"
        props[TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG] = "CreateTime"

        val arrayOfTopicName = kafkaProp.topics.listOfTopics.toTypedArray()

        val arrayOfTopic = ArrayList<NewTopic>(arrayOfTopicName.size)
        for (topicName in arrayOfTopicName) {
            val topic = TopicBuilder.name(topicName)
                .replicas(kafkaProp.topics.replicationFactor.toInt())
                .partitions(kafkaProp.topics.numberOfPartitions)
                .configs(props)
                .build()
            arrayOfTopic.add(topic)
        }
        //Spread operator
        return KafkaAdmin.NewTopics(*arrayOfTopic.toTypedArray())
    }
}