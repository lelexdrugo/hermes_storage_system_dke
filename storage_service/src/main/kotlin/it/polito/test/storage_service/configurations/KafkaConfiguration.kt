package it.polito.test.storage_service.configurations

import it.polito.test.storage_service.dtos.KafkaPayloadDTO
import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.health.AbstractHealthIndicator
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import org.springframework.core.env.Profiles
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.*
import org.springframework.kafka.core.KafkaAdmin.NewTopics
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.messaging.Message
import kotlin.properties.Delegates


//https://kafka.apache.org/documentation


/**
 * Consumer configuration
 */
@EnableKafka
@Configuration
class KafkaConsumerConfig(private val kafkaProp: KafkaProperties) {
    private val logger = KotlinLogging.logger {}
    var listenerStartupTime by Delegates.notNull<Long>()

    /**
     * auto invoke this method to start the listener in normal mode
     */
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry
    @Autowired
    var env: Environment? = null
    @EventListener(ApplicationReadyEvent::class)
    fun init() {
        if (env!!.acceptsProfiles(Profiles.of("!benchmarkTest"))) {
            startListener()
            //kafkaListenerEndpointRegistry.getListenerContainer("unique")!!.start()
        }
    }

    fun startListener() {
        listenerStartupTime = System.currentTimeMillis()
        if(kafkaProp.batchListener.enabled)
            if(kafkaProp.batchListener.aggregation)
                kafkaListenerEndpointRegistry.getListenerContainer("batchAggregateMessageListener")!!.start().also {  logger.info { "Starting batch listener with aggregation" } }
            else
                kafkaListenerEndpointRegistry.getListenerContainer("batchSimplyForwardListener")!!.start().also { logger.info { "Starting batch listener without aggregation" }}
        else
            kafkaListenerEndpointRegistry.getListenerContainer("oneByOneListener")!!.start().also { logger.info { "Starting non-batch listener" } }

        logger.info { "Listener started at: $listenerStartupTime ms" }

    }

    fun isListenerRunning(): Boolean {
        return kafkaListenerEndpointRegistry.allListenerContainers.stream().anyMatch { it.isRunning }
    }

    fun stopListeners() {
        logger.info { "Stopping listener" }
        kafkaListenerEndpointRegistry.allListenerContainers.forEach { it.stop() }
    }

    fun listenerStatus(): Status {
        return if (isListenerRunning()) Status.UP else Status.DOWN
    }

    @Bean
    fun kafkaHealthIndicator(): HealthIndicator? {
        return object : AbstractHealthIndicator() {
            override fun doHealthCheck(builder: Health.Builder) {
                builder.status(listenerStatus())
                    .build()
            }
        }
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, Message<Any>> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProp.bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = kafkaProp.consumer.groupId
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = kafkaProp.consumer.keyDeserializer
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = kafkaProp.consumer.valueDeserializer
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = kafkaProp.consumer.autoOffsetReset
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = kafkaProp.consumer.enableAutoCommit
        props[JsonDeserializer.TRUSTED_PACKAGES] = kafkaProp.consumer.trustedPackage
        props[JsonDeserializer.USE_TYPE_INFO_HEADERS] = false
        props[JsonDeserializer.VALUE_DEFAULT_TYPE] = KafkaPayloadDTO::class.java
//        props[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = false
        //props[JsonDeserializer.TRUSTED_PACKAGES] = "*"
        //props[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = "org.apache.kafka.clients.consumer.RoundRobinAssignor"
//        props[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 60000 //Default 45000
//        props[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = 10000 //Default 3000

        //The maximum amount of memory a client can consume is calculated approximately as:
        //NUMBER-OF-BROKERS * fetch.max.bytes and NUMBER-OF-PARTITIONS * max.partition.fetch.bytes

        // The maximum number of records returned in a single call to poll(). Note, that max.poll.records does not impact the underlying fetching behavior.
        // The consumer will cache the records from each fetch request and returns them incrementally from each poll.
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = kafkaProp.consumer.maxPoolRecords //Default 500

        //Improving throughput by increasing the minimum amount of data fetched in a request
        //To ensure larger batch and make less request
//        props[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 16384 //Default 1
//        props[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 10000 //Default 500

        //Lowering latency by increasing maximum batch sizes
        // Note that the consumer performs multiple fetches in parallel.
        // Note  this is not an absolute maximum. The maximum record batch size accepted by the broker is defined via message.max.bytes (broker config) or max.message.bytes (topic config)
        props[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = kafkaProp.consumer.fetchMaxBytes //Default 52428800 50MB amount of data fetched from the broker at one time
        props[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = kafkaProp.consumer.maxPartitionFetchBytes //Default 1048576 1MB

        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Message<Any>> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Message<Any>>()
        factory.consumerFactory = consumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.setConcurrency(kafkaProp.concurrencyListener)
        factory.isBatchListener = kafkaProp.batchListener.enabled
        //factory.containerProperties.isSyncCommits = true
        return factory
    }
}

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
            configProps[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = kafkaProp.producer.maxRequestSize
            //Default:	33554432. This value can cause OOM error in the producer.
//            configProps[ProducerConfig.BUFFER_MEMORY_CONFIG] = 52428800
//            Default: 60000
//            configProps[ProducerConfig.MAX_BLOCK_MS_CONFIG] = 1200000
            val dpf = DefaultKafkaProducerFactory<String,Message<Any>>(configProps)
            //https://stackoverflow.com/questions/64389445/proceeding-to-force-close-the-producer-since-pending-requests-could-not-be-compl
            dpf.setPhysicalCloseTimeout(300)
            dpf.isProducerPerThread = true

            return dpf//DefaultKafkaProducerFactory(configProps)
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
    //Strano... spring boot dovrebbe creare automaticamente un bean kafka admin, però è come se non funzionasse quando specifico io i topics
    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs: MutableMap<String, Any?> = HashMap()
        configs[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProp.bootstrapServers
        return KafkaAdmin(configs)
    }
    //when you run a single node Kafka cluster for local development, there are no other brokers to replicate the data.
    // So you can only configure a replication factor of one!

    var topicCollection = ArrayList<NewTopic>()
    @Bean
    fun topics(): NewTopics {
        val props: MutableMap<String, String> = HashMap()
        //Default for kafka:	1048588
        props[TopicConfig.MAX_MESSAGE_BYTES_CONFIG] = kafkaProp.topics.maxMessageBytes.toString()
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
        topicCollection = arrayOfTopic
        //Spread operator
        return NewTopics(*arrayOfTopic.toTypedArray())
    }
}
