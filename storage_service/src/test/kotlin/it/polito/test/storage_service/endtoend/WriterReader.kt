package it.polito.test.storage_service.endtoend

import it.polito.test.storage_service.client.driver.DriverImpl
import it.polito.test.storage_service.client.generator.Generator
import it.polito.test.storage_service.client.validator.Validator
import it.polito.test.storage_service.configurations.KafkaProperties
import it.polito.test.storage_service.dtos.FileResourceBlockDTO
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import mu.KotlinLogging
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.client.exchange
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.http.*
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.RuntimeException
import kotlin.concurrent.thread
import kotlin.io.path.Path
import kotlin.random.Random


@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WriterReader {
    companion object{
        @Container
        var mongoContainer: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
        @Container
        var kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1")).apply {
            this.start()
        }

//        var mongoContainerReplicated = MongoContainer().apply { this.use() }



        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
//            registry.add("spring.data.mongodb.uri", mongoContainerReplicated::getReplicaSetUrl)
            registry.add("spring.data.mongodb.uri", mongoContainer::getReplicaSetUrl)
            registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers)
        }
    }

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, Message<Any>>
    @Autowired
    private lateinit var kafkaProperties: KafkaProperties

    @Autowired
    private lateinit var restTemplate: TestRestTemplate

    @Autowired
    lateinit var mongoDb: MongoTemplate

    val driver = DriverImpl()

    val filename = Random.nextLong().toString()

    private val logger = KotlinLogging.logger {}

    @BeforeEach
    fun setUp() {
        mongoDb.db.getCollection("rcs.files").drop()
        mongoDb.db.getCollection("rcs.chunks").drop()
/*        mongoDb.db.getCollection("rcs.chunks").createIndex(
            Document("files_id", 1).append("n", -1),
            IndexOptions().unique(true)
        )*/
    }

    @Test
    fun `multiple write, single read`(){

        val dimension = 100L
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val numberOfWrite = 3
        for (i in 1..numberOfWrite){
            val content = generator.generate(dimension)
            driver.writeOnDisk(
                kafkaTemplate,
                kafkaProperties.topics.listOfTopics[0],
                filename,
                content,
                metadata)
        }
        val totalDimension = dimension * numberOfWrite

        //wait for the messages to be consumed
        Thread.sleep(5000)

        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=$filename",
            HttpMethod.GET,
        )
        assertNotNull(response.body, "The response is null")
        assertEquals(HttpStatus.OK, response.statusCode, "Status code is ${response.statusCode} and not ${HttpStatus.OK}")
        assertEquals(totalDimension, response.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        assertEquals(totalDimension, response.body!!.file.length, "The length of the file is not the same")
        assertEquals(metadata["contentType"], response.body!!.file.metadata["_contentType"], "The field contentType of metadata is not the same")

        val validator = Validator(filename)
        val result = validator.validate(response.body!!.data)
        assertTrue(result){
            "Validation failed. The content of the file is not the same"
        }
    }

    /**
     * Steps:
     *  - Simulate a client: through the driver, powered by generator, write on disk
     *  - then simulate a client that read through rest endpoint and use method validate of class validator (instantiate with the same filename) to check the result
     */

    @Test
    fun `single write, single read`(){
        val dimension = 100L
        val generator = Generator(filename)
        val content = generator.generate(dimension)
        val metadata = mutableMapOf("contentType" to "document/docx")
        driver.writeOnDisk(
            kafkaTemplate,
            kafkaProperties.topics.listOfTopics[0],
            filename,
            content,
            metadata)

        //wait for the message to be consumed
        Thread.sleep(5000)

        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=$filename",
            HttpMethod.GET,
        )
        assertNotNull(response.body, "The response is null")
        assertEquals(HttpStatus.OK, response.statusCode, "Status code is ${response.statusCode} and not ${HttpStatus.OK}")
        assertEquals(dimension, response.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        assertEquals(dimension, response.body!!.file.length, "The length of the file is not the same")
        assertEquals(metadata["contentType"], response.body!!.file.metadata["_contentType"], "The field contentType of metadata is not the same")

        val validator = Validator(filename)
        val result = validator.validate(response.body!!.data)
        assertTrue(result){
            "Validation failed. The content of the file is not the same"
        }
    }

    @Test
    fun `multiple write, multiple read to obtain the entire file`() {
    //Contact the endpoint until the file is complete
        //Start reading after the first write
        val dimension = 100L
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val numberOfWrite = 4
        for (i in 1..numberOfWrite-1){
            val content = generator.generate(dimension)
            driver.writeOnDisk(
                kafkaTemplate,
                kafkaProperties.topics.listOfTopics[0],
                filename,
                content,
                metadata)
        }
        val partialDimension = dimension * (numberOfWrite-1)
        val totalDimension = dimension * numberOfWrite

        //wait for the messages to be consumed
        Thread.sleep(5000)

        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=$filename",
            HttpMethod.GET,
        )
        assertNotNull(response.body, "The response is null")
        assertEquals(HttpStatus.OK, response.statusCode, "Status code is ${response.statusCode} and not ${HttpStatus.OK}")
        assertEquals(partialDimension, response.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        assertEquals(partialDimension, response.body!!.file.length, "The length of the file is not the same")
        assertEquals(metadata["contentType"], response.body!!.file.metadata["_contentType"], "The field contentType of metadata is not the same")


        //Assert that the file is not complete
        assertNotEquals(totalDimension, response.body!!.data.size.toLong(), "The file is complete")
        assertNotEquals(totalDimension, response.body!!.file.length, "The file is complete")

        //Validate the content obtained until now
        val validator = Validator(filename)
        val result = validator.validate(response.body!!.data)
        assertTrue(result){
            "Validation failed. The content of the file is not the same"
        }

        //Write the last block
        val content = generator.generate(dimension)
        driver.writeOnDisk(
            kafkaTemplate,
            kafkaProperties.topics.listOfTopics[0],
            filename,
            content,
            metadata)
        //wait for the message to be consumed
        Thread.sleep(5000)
        //Check that the file is complete
        val response2: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=$filename",
            HttpMethod.GET,
        )
        assertNotNull(response2.body, "The response is null")
        assertEquals(HttpStatus.OK, response.statusCode, "Status code is ${response.statusCode} and not ${HttpStatus.OK}")
        assertEquals(totalDimension, response2.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response2.body!!.file.filename, "The filename is not the same")
        assertEquals(totalDimension, response2.body!!.file.length, "The length of the file is not the same")
        assertEquals(metadata["contentType"], response2.body!!.file.metadata["_contentType"], "The field contentType of metadata is not the same")

        //I have request again the entire file
        //So I must seek backward the validator
        //In another test I can use the endpoint specifying offset and so use the validator without reset it
        //Validate again the content
        validator.seekToPosition(0)
        val result2 = validator.validate(response2.body!!.data)
        assertTrue(result2){
            "Second validation failed. The content of the file is not the same"
        }
    }

    /**
     * Try reading without waiting for the write to be completed
     * What happen contacting the endpoint before the file is created/complete?
     */
    @Test
    fun `multiple write, multiple read to obtain the entire file without waiting for the write to be completed`() {
        //Contact the endpoint until the file is complete
        //Start reading after the first write
        val dimension = 100*KIBIBYTE
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val numberOfWrite = 100
        for (i in 1..numberOfWrite) {
            val content = generator.generate(dimension)
            driver.writeOnDisk(
                kafkaTemplate,
                kafkaProperties.topics.listOfTopics[0],
                filename,
                content,
                metadata
            )
        }
        val totalDimension = dimension * numberOfWrite

        var repeat: Boolean
        var response: ResponseEntity<FileResourceBlockDTO>
        do {
            response = restTemplate.exchange(
                "/files/?filename=$filename",
                HttpMethod.GET,
            )
            repeat = if(response.statusCode == HttpStatus.NOT_FOUND){
                logger.debug("The file is not yet created")
                true
            } else if(response.statusCode == HttpStatus.OK){
                if(response.body!!.file.length != totalDimension){
                    logger.debug("The file is not yet complete")
                    true
                } else {
                    logger.debug("The file is complete")
                    false
                }
            } else {
                logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                true
            }
        }while (repeat)
        assertNotNull(response.body, "The response is null")
        assertEquals(HttpStatus.OK, response.statusCode, "Status code is ${response.statusCode} and not ${HttpStatus.OK}")
        assertEquals(totalDimension, response.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        assertEquals(totalDimension, response.body!!.file.length, "The length of the file is not the same")
        assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        //Validate the content
        val validator = Validator(filename)
        val result = validator.validate(response.body!!.data)
        assertTrue(result){
            "Validation failed. The content of the file is not the same"
        }
    }

    @Test
    fun `single write empty document, single read`() {
        val dimension = 0L
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val content = generator.generate(dimension)
        driver.writeOnDisk(
            kafkaTemplate,
            kafkaProperties.topics.listOfTopics[0],
            filename,
            content,
            metadata
        )
        //wait for the message to be consumed
        Thread.sleep(5000)
        val response: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=$filename",
            HttpMethod.GET,
        )
        assertNotNull(response.body, "The response is null")
        assertEquals(dimension, response.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        assertEquals(dimension, response.body!!.file.length, "The length of the file is not the same")
        assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        //Validate the content
        val validator = Validator(filename)
        val result = validator.validate(response.body!!.data)
        assertTrue(result){
            "Validation failed. The content of the file is not the same"
        }
    }

    @Test
    fun `multiple write, multiple read with offset to obtain the entire file without waiting for the write to be completed`() {
        //Contact the endpoint until the file is complete
        //Start reading after the first write
        val dimension = 100L
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val numberOfWrite = 100
        for (i in 1..numberOfWrite) {
            val content = generator.generate(dimension)
            driver.writeOnDisk(
                kafkaTemplate,
                kafkaProperties.topics.listOfTopics[0],
                filename,
                content,
                metadata
            )
        }
        val totalDimension = dimension * numberOfWrite

        var repeat: Boolean
        var response: ResponseEntity<FileResourceBlockDTO>
        var validatedByte=0
        //Validate the content piece by piece
        val validator = Validator(filename)
        do {
            response = restTemplate.exchange(
                "/files/?filename=$filename&offset=$validatedByte",
                HttpMethod.GET,
            )
            repeat = if(response.statusCode == HttpStatus.NOT_FOUND){
                logger.info("The file is not yet created")
                true
            } else if(response.statusCode == HttpStatus.OK){
                if(validatedByte.toLong() != totalDimension){
                    logger.info("The file is not yet complete")
                    val result = validator.validate(response.body!!.data)
                    assertTrue(result){
                        "Validation failed. The content of the file is not the same"
                    }
                    validatedByte+=response.body!!.data.size

                    assertNotNull(response.body, "The response is null")
                    assertEquals(HttpStatus.OK, response.statusCode, "Status code is ${response.statusCode} and not ${HttpStatus.OK}")
                    assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
                    assertEquals(
                        metadata["contentType"],
                        response.body!!.file.metadata["_contentType"],
                        "The field contentType of metadata is not the same"
                    )
                    //Return true if the file is not yet complete
                    validatedByte.toLong() != totalDimension
                    //true
                } else {
                    logger.info("The file is complete")
                    false
                }
            } else {
                logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                true
            }
        }while (repeat)
        assertNotNull(response.body, "The response is null")
        assertEquals(HttpStatus.OK, response.statusCode, "Status code is ${response.statusCode} and not ${HttpStatus.OK}")
        assertEquals(validatedByte.toLong(), response.body!!.file.length)
        assertEquals(filename, response.body!!.file.filename, "The filename is not the same")
        assertEquals(totalDimension, response.body!!.file.length, "The length of the file is not the same")
        assertEquals(
            metadata["contentType"],
            response.body!!.file.metadata["_contentType"],
            "The field contentType of metadata is not the same"
        )
        //Validate the remaining content
        val result = validator.validate(response.body!!.data.drop(validatedByte).toByteArray())
        assertTrue(result){
            "Validation failed. The content of the file is not the same"
        }
    }

    /**
     * Seek reading the file
     */
    @Test
    fun `write, read, seek backward, write, seek forward`(){
        val dimension = 100L
        val generator = Generator(filename)
        val content = generator.generate(dimension)
        val metadata = mutableMapOf("contentType" to "document/docx")
        driver.writeOnDisk(
            kafkaTemplate,
            kafkaProperties.topics.listOfTopics[0],
            filename,
            content,
            metadata)

        //wait for the message to be consumed
        //Thread.sleep(5000)

        var repeat: Boolean
        var response1: ResponseEntity<FileResourceBlockDTO>
        do {
            response1 = restTemplate.exchange(
                "/files/?filename=$filename",
                HttpMethod.GET,
            )
            repeat = if(response1.statusCode == HttpStatus.NOT_FOUND){
                logger.info("The file is not yet created")
                true
            } else if(response1.statusCode == HttpStatus.OK){
                false
            } else {
                logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response1.statusCode}")
                true
            }
        }while (repeat)

        assertNotNull(response1.body, "The response is null")
        assertEquals(HttpStatus.OK, response1.statusCode, "Status code is ${response1.statusCode} and not ${HttpStatus.OK}")
        assertEquals(dimension, response1.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response1.body!!.file.filename, "The filename is not the same")
        assertEquals(dimension, response1.body!!.file.length, "The length of the file is not the same")
        assertEquals(metadata["contentType"], response1.body!!.file.metadata["_contentType"], "The field contentType of metadata is not the same")

        val validator = Validator(filename)
        val result1 = validator.validate(response1.body!!.data)
        assertTrue(result1){
            "First validation failed. The content of the file is not the same"
        }

        //Seek backward
        val offset2 = dimension/2
        val response2: ResponseEntity<FileResourceBlockDTO> =restTemplate.exchange(
            "/files/?filename=$filename&offset=$offset2",
            HttpMethod.GET,
        )
        assertNotNull(response2.body, "The response is null")
        assertEquals(HttpStatus.OK, response2.statusCode, "Status code is ${response2.statusCode} and not ${HttpStatus.OK}")
        assertEquals(dimension-offset2, response2.body!!.data.size.toLong(), "The size of the file is not the same")
        assertEquals(filename, response2.body!!.file.filename, "The filename is not the same")
        assertEquals(dimension, response2.body!!.file.length, "The length of the file is not the same")
        assertEquals(metadata["contentType"], response2.body!!.file.metadata["_contentType"], "The field contentType of metadata is not the same")

        validator.seekToPosition(offset2.toInt())
        val result2 = validator.validate(response2.body!!.data)
        assertTrue(result2){
            "Second validation failed. The content of the file is not the same"
        }
    }

    /**
     * Multiple file creation
     */

    val KIBIBYTE = 1024L
    val MEBIBYTE = 1024 * KIBIBYTE

    @Test
    fun `write file using generator function`(){
        val numberOfFiles = 10
        val maxDimension = 10*MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        for (file in files){
            logger.info("I'm generating the file ${file.key}")
            val gen = Generator(file.key)
            val content = gen.generate(file.value)
            driver.writeOnDisk(
                kafkaTemplate,
                kafkaProperties.topics.listOfTopics[0],
                file.key,
                content,
                metadata
            )
        }
        /*        //Create a map of validator
                val validators = mutableMapOf<String, Validator>()
                for (file in files){
                    validators[file.key] = Validator(file.key)
                }*/
        for (file in files){
            logger.info("I'm validating the file ${file.key}")
            var repeat: Boolean
            var response: ResponseEntity<FileResourceBlockDTO>
            var validatedByte = 0
            //Validate the content piece by piece
            val validator = Validator(file.key)
            val totalDimension = file.value
            do {
                response = restTemplate.exchange(
                    "/files/?filename=${file.key}&offset=$validatedByte",
                    HttpMethod.GET,
                )
                repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                    logger.info("The file is not yet created")
                    true
                } else if (response.statusCode == HttpStatus.OK) {
                    if (validatedByte.toLong() != totalDimension) {
                        logger.info("The file is not yet complete")
                        val result = validator.validate(response.body!!.data)
                        assertTrue(result) {
                            "Validation failed. The content of the file is not the same"
                        }
                        validatedByte += response.body!!.data.size

                        assertNotNull(response.body, "The response is null")
                        assertEquals(
                            HttpStatus.OK,
                            response.statusCode,
                            "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                        )
                        assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                        assertEquals(
                            metadata["contentType"],
                            response.body!!.file.metadata["_contentType"],
                            "The field contentType of metadata is not the same"
                        )
                        //Return true if the file is not yet complete
                        validatedByte.toLong() != totalDimension
                        //true
                    } else {
                        logger.info("The file is complete")
                        false
                    }
                } else {
                    logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                    true
                }
                Thread.sleep(1000)
            } while (repeat)
        }
    }

    @Test
    fun `coroutines test`() = runTest {
        launch {
            delay(1_000)
            println("1. $currentTime") // 1000
            delay(200)
            println("2. $currentTime") // 1200
            delay(2_000)
            println("4. $currentTime") // 3200
        }
        val deferred = async {
            delay(3_000)
            println("3. $currentTime") // 3000
            delay(500)
            println("5. $currentTime") // 3500
        }
        deferred.await()
    }
    @Test
    fun `coroutines test my app`() = runTest {
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val numberOfFiles = 10
        val maxDimension = 10*MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val generation = mutableMapOf<String, Deferred<Unit>>()
        for (file in files){
            generation[file.key] = async{
                println("I'm generating the file ${file.key}")
                val driverLocal = DriverImpl()
                val gen = Generator(file.key)
                var generatedDimension = 0L
                while(generatedDimension<file.value){
                    var pieceDimension = Random.nextLong(maxRequestSize-100L, (2*maxRequestSize).toLong())
                    if(generatedDimension+pieceDimension>file.value){
                        pieceDimension=file.value-generatedDimension
                    }
                    val content = gen.generate(pieceDimension)
                    driverLocal.writeOnDisk(
                        kafkaTemplate,
                        kafkaProperties.topics.listOfTopics[0],
                        file.key,
                        content,
                        metadata)
                    generatedDimension+= pieceDimension
                }
                println("Finish to send message for: ${file.key}")
            }
        }
/*
        for (file in files)
            generation[file.key]!!.await()*/


        /*        //Create a map of validator
                val validators = mutableMapOf<String, Validator>()
                for (file in files){
                    validators[file.key] = Validator(file.key)
                }*/
        val validation = mutableMapOf<String, Deferred<Unit>>()
        for (file in files){
            validation[file.key] = async{
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<FileResourceBlockDTO>
                var validatedByte = 0
                //Validate the content piece by piece
                val validator = Validator(file.key)
                val totalDimension = file.value
                do {
                    response = restTemplate.exchange(
                        "/files/?filename=${file.key}&offset=$validatedByte",
                        HttpMethod.GET,
                    )
                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        println("The file is not yet created")
                        true
                    } else if (response.statusCode == HttpStatus.OK) {
                        if (validatedByte.toLong() != totalDimension) {
                            println("The file is not yet complete")
                            val result = validator.validate(response.body!!.data)
                            assertTrue(result) {
                                "Validation failed. The content of the file is not the same"
                            }
                            validatedByte += response.body!!.data.size

                            assertNotNull(response.body, "The response is null")
                            assertEquals(
                                HttpStatus.OK,
                                response.statusCode,
                                "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                            )
                            assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                            assertEquals(
                                metadata["contentType"],
                                response.body!!.file.metadata["_contentType"],
                                "The field contentType of metadata is not the same"
                            )
                            //Return true if the file is not yet complete
                            validatedByte.toLong() != totalDimension
                            //true
                        } else {
                            println("The file is complete")
                            false
                        }
                    } else {
                        println("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
                    Thread.sleep(1000)
                } while (repeat)
            }

        }
        for (file in files)
        validation[file.key]!!.await()
    }

    @Test
    fun `write multiple file, generating and validating with multithread`(){
        val numberOfFiles = 10
        val maxDimension = 10*MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val generation = mutableMapOf<String, Thread>()
        for (file in files){
            generation[file.key] = thread(start = true){
                println("I'm generating the file ${file.key}")
                val driverLocal = DriverImpl()
                val gen = Generator(file.key)
                val content = gen.generate(file.value)
                driverLocal.writeOnDisk(
                    kafkaTemplate,
                    kafkaProperties.topics.listOfTopics[0],
                    file.key,
                    content,
                    metadata
                )

                println("Finish to send message for: ${file.key}")
            }
        }
/*        //Create a map of validator
        val validators = mutableMapOf<String, Validator>()
        for (file in files){
            validators[file.key] = Validator(file.key)
        }*/
        val validation = mutableMapOf<String, Thread>()
        for (file in files){
            validation[file.key] = thread(start = true){
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<FileResourceBlockDTO>
                var validatedByte = 0
                //Validate the content piece by piece
                val validator = Validator(file.key)
                val totalDimension = file.value
                do {
                    response = restTemplate.exchange(
                        "/files/?filename=${file.key}&offset=$validatedByte",
                        HttpMethod.GET,
                    )
                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        println("The file is not yet created")
                        true
                    } else if (response.statusCode == HttpStatus.OK) {
                        if (validatedByte.toLong() != totalDimension) {
                            println("The file is not yet complete")
                            val result = validator.validate(response.body!!.data)
                            assertTrue(result) {
                                "Validation failed. The content of the file is not the same"
                            }
                            validatedByte += response.body!!.data.size

                            assertNotNull(response.body, "The response is null")
                            assertEquals(
                                HttpStatus.OK,
                                response.statusCode,
                                "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                            )
                            assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                            assertEquals(
                                metadata["contentType"],
                                response.body!!.file.metadata["_contentType"],
                                "The field contentType of metadata is not the same"
                            )
                            //Return true if the file is not yet complete
                            validatedByte.toLong() != totalDimension
                            //true
                        } else {
                            println("The file is complete")
                            false
                        }
                    } else {
                        println("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
                    Thread.sleep(1000)
                } while (repeat)
            }
        }

        for (file in files) {
            generation[file.key]!!.join()
            validation[file.key]!!.join()
        }
    }

    //If I choose a small dimension for the minimum size of the piece we create too many messages. Test needs a lot of time
    @Test
    fun `write file using generator function, calling generator multiple times, with value around the max size accepted by kafka`(){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val numberOfFiles = 10
        val maxDimension = 100*MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        for (file in files){
            logger.info("I'm generating the file ${file.key}")
            val gen = Generator(file.key)
            var generatedDimension = 0L
            while(generatedDimension<file.value){
                var pieceDimension = Random.nextLong(maxRequestSize-100L, (2*maxRequestSize).toLong())
                if(generatedDimension+pieceDimension>file.value){
                    pieceDimension=file.value-generatedDimension
                }
                val content = gen.generate(pieceDimension)
                driver.writeOnDisk(
                    kafkaTemplate,
                    kafkaProperties.topics.listOfTopics[0],
                    file.key,
                    content,
                    metadata)
                generatedDimension+= pieceDimension
            }
        }
        for (file in files){
            logger.info("I'm validating the file ${file.key}")
            var repeat = true
            var response: ResponseEntity<FileResourceBlockDTO>
            var validatedByte=0
            //Validate the content piece by piece
            val validator = Validator(file.key)
            val totalDimension = file.value
            //Another way to do the same thing
            while(repeat){
                response = restTemplate.exchange(
                    "/files/?filename=${file.key}&offset=$validatedByte",
                    HttpMethod.GET,
                )
                when (response.statusCode){
                    HttpStatus.NOT_FOUND -> {
                        logger.info("The file is not yet created")
                        repeat = true
                    }
                    HttpStatus.OK -> {
                        if (validatedByte.toLong() != totalDimension) {
                            logger.info("The file is not yet complete")
                            val result = validator.validate(response.body!!.data)
                            assertTrue(result) {
                                "Validation failed. The content of the file is not the same"}
                            validatedByte += response.body!!.data.size

                            assertNotNull(response.body, "The response is null")
                            assertEquals(
                                HttpStatus.OK,
                                response.statusCode,
                                "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                            )
                            assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                            assertEquals(
                                metadata["contentType"],
                                response.body!!.file.metadata["_contentType"],
                                "The field contentType of metadata is not the same"
                            )
                            //Return true if the file is not yet complete
                            validatedByte.toLong() != totalDimension
                            //true
                        } else {
                            logger.info("The file is complete")
                            repeat = false
                        }
                    }
                    else -> {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        repeat = true
                    }
                }
                //Thread.sleep(1000)
            }
        }
    }

    @Test
    fun `write multiple file, calling generator multiple times, with value around the max size accepted by kafka, generating and validating with multithread`(){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val numberOfFiles = 5
        val maxDimension = 1000*MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()

        for (file in files){
            thread(start = true){
                println("I'm generating the file ${file.key}")
                val driverLocal = DriverImpl()
                val gen = Generator(file.key)
                var generatedDimension = 0L
                while(generatedDimension<file.value){
                    var pieceDimension = Random.nextLong(maxRequestSize-100L, (2*maxRequestSize).toLong())
                    if(generatedDimension+pieceDimension>file.value){
                        pieceDimension=file.value-generatedDimension
                    }
                    val content = gen.generate(pieceDimension)
                    driverLocal.writeOnDisk(
                        kafkaTemplate,
                        kafkaProperties.topics.listOfTopics[0],
                        file.key,
                        content,
                        metadata)
                    generatedDimension+= pieceDimension
                }

                println("Finish to send message for: ${file.key}")
            }
        }
        /*        //Create a map of validator
                val validators = mutableMapOf<String, Validator>()
                for (file in files){
                    validators[file.key] = Validator(file.key)
                }*/
        val validation = mutableMapOf<String, Thread>()
        for (file in files){
            validation[file.key] = thread(start = true){
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<FileResourceBlockDTO>
                var validatedByte = 0
                //Validate the content piece by piece
                val validator = Validator(file.key)
                val totalDimension = file.value
                do {
                    response = restTemplate.exchange(
                        "/files/?filename=${file.key}&offset=$validatedByte",
                        HttpMethod.GET,
                    )
                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        logger.trace("The file: $filename is not yet created")
                        true
                    } else if (response.statusCode == HttpStatus.OK) {
                        if (validatedByte.toLong() != totalDimension) {
                            logger.trace("The file: $filename is not yet complete")
                            val result = validator.validate(response.body!!.data)
                            assertTrue(result) {
                                "Validation failed. The content of the file is not the same"
                            }
                            validatedByte += response.body!!.data.size

                            assertNotNull(response.body, "The response is null")
                            assertEquals(
                                HttpStatus.OK,
                                response.statusCode,
                                "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                            )
                            assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                            assertEquals(
                                metadata["contentType"],
                                response.body!!.file.metadata["_contentType"],
                                "The field contentType of metadata is not the same"
                            )
                            //Return true if the file is not yet complete
                            validatedByte.toLong() != totalDimension
                            //true
                        } else {
                            logger.info("The file: $filename is complete")
                            false
                        }
                    } else {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
//                    Thread.sleep(1000)
                } while (repeat)
            }
        }

        for (file in files)
            validation[file.key]!!.join()
    }



    @Test
    fun `write multiple file, calling generator multiple times, with value around the max size accepted by kafka, generating and multithread validation`(){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val numberOfFiles = 100
        val maxDimension = 100*MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = false)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        //Genero in un thread a parte così comincio a validare prima di aver finito di generare
        thread (start = true){
            for (file in files){
                    println("I'm generating the file ${file.key}")
                    val driverLocal = DriverImpl()
                    val gen = Generator(file.key)
                    var generatedDimension = 0L
                    while(generatedDimension<file.value){
                        var pieceDimension = Random.nextLong(maxRequestSize-100L, (2*maxRequestSize).toLong())
                        if(generatedDimension+pieceDimension>file.value){
                            pieceDimension=file.value-generatedDimension
                        }
                        val content = gen.generate(pieceDimension)
                        driverLocal.writeOnDisk(
                            kafkaTemplate,
                            kafkaProperties.topics.listOfTopics[0],
                            file.key,
                            content,
                            metadata)
                        generatedDimension+= pieceDimension
                    }

                    println("Finish to send message for: ${file.key}")
            }
        }
        /*        //Create a map of validator
                val validators = mutableMapOf<String, Validator>()
                for (file in files){
                    validators[file.key] = Validator(file.key)
                }*/
        val validation = mutableMapOf<String, Thread>()
        for (file in files){
            validation[file.key] = thread(start = true){
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<FileResourceBlockDTO>
                var validatedByte = 0
                //Validate the content piece by piece
                val validator = Validator(file.key)
                val totalDimension = file.value
                do {
                    response = restTemplate.exchange(
                        "/files/?filename=${file.key}&offset=$validatedByte",
                        HttpMethod.GET,
                    )
                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        logger.trace("The file: $filename is not yet created")
                        //Creando 100 file, con mono thread, i messaggi vengono mandati sequenzialmente per un unico file.
                        //Quindi lato client verranno consumati più velocemente (ho molti thread), in relazione ai messaggi ricevuti per differenti filename
                        //Quindi validando avrò magari 90/95 thread che loopano su questo primo if (ancora il file non è stato creato) per molto tempo.
                        //Meglio metterli a dormire. Avrebbe molto senso, in questo specifico contesto, usare le coroutine e fare delle delay, così da venire sospesi
                        Thread.sleep(2000)
                        true
                    } else if (response.statusCode == HttpStatus.OK) {
                        if (validatedByte.toLong() != totalDimension) {
                            logger.trace("The file: $filename is not yet complete")
                            val result = validator.validate(response.body!!.data)
                            assertTrue(result) {
                                "Validation failed. The content of the file is not the same"
                            }
                            validatedByte += response.body!!.data.size

                            assertNotNull(response.body, "The response is null")
                            assertEquals(
                                HttpStatus.OK,
                                response.statusCode,
                                "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                            )
                            assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                            assertEquals(
                                metadata["contentType"],
                                response.body!!.file.metadata["_contentType"],
                                "The field contentType of metadata is not the same"
                            )
                            //Return true if the file is not yet complete
                            validatedByte.toLong() != totalDimension
                            //true
                        } else {
                            logger.info("The file: $filename is complete")
                            false
                        }
                    } else {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
//                    Thread.sleep(1000)
                } while (repeat)
            }
        }

        for (file in files)
            validation[file.key]!!.join()
    }


    @Test
    fun `write multiple file, calling generator multiple times, with value around the max size accepted by kafka, generating not so multi and multithread validation`(){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val numberOfFiles = 50
        val maxDimension = 512*MEBIBYTE
        val files = Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val threadRatio = 10

        val failedGenerationFlag = AtomicBoolean(false)
        val failedValidationFlag = AtomicBoolean(false)
        //Genero in un thread a parte così comincio a validare prima di aver finito di generare
        //Problem with exception throws inside
        //thread (start = true) {
        val generation = mutableMapOf<String, Future<*>>()
        val nOfGeneratorWorker= (numberOfFiles/threadRatio).coerceAtMost(25).coerceAtLeast(15)
        val workerGenerator = Executors.newFixedThreadPool(nOfGeneratorWorker, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "generator-thread-${counter.incrementAndGet()}")
        })
        for (file in files) {
            generation[file.key] = workerGenerator.submit {
               try  {
                    println("I'm generating the file ${file.key}")
                    val driverLocal = DriverImpl()
                    val gen = Generator(file.key)
                    var generatedDimension = 0L
                    while (generatedDimension < file.value) {
//                    var pieceDimension = Random.nextLong(maxRequestSize - 100L, (2 * maxRequestSize).toLong())
                        var pieceDimension =
                            Random.nextLong((0.5 * maxRequestSize).toLong(), (1.1 * maxRequestSize).toLong())

                        if (generatedDimension + pieceDimension > file.value) {
                            pieceDimension = file.value - generatedDimension
                        }
                        val content = gen.generate(pieceDimension)
                        driverLocal.writeOnDisk(
                            kafkaTemplate,
                            kafkaProperties.topics.listOfTopics[0],
                            file.key,
                            content,
                            metadata
                        )
                        generatedDimension += pieceDimension
                    }
                    println("Finish to send message for: ${file.key}")
                }
               catch (e: Exception){
                   failedGenerationFlag.set(true)
                   throw e
               }
            }
        }
        //}

        val nOfValidatorWorker = 200.coerceAtMost(numberOfFiles)
        val workerValidator = Executors.newFixedThreadPool(nOfValidatorWorker, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        val validation = mutableMapOf<String, Future<*>>()
        for (file in files){
            validation[file.key] = workerValidator.submit{
              try  {
                    println("I'm validating the file ${file.key}")
                    var repeat: Boolean
                    var response: ResponseEntity<FileResourceBlockDTO>
                    var validatedByte = 0
                    //Validate the content piece by piece
                    val validator = Validator(file.key)
                    val totalDimension = file.value
                    do {
                        response = restTemplate.exchange(
                            "/files/?filename=${file.key}&offset=$validatedByte",
                            HttpMethod.GET,
                        )
                        repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                            logger.trace("The file: ${file.key}  is not yet created")
                            //Creando 100 file, con mono thread, i messaggi vengono mandati sequenzialmente per un unico file.
                            //Quindi lato client verranno consumati più velocemente (ho molti thread), in relazione ai messaggi ricevuti per differenti filename
                            //Quindi validando avrò magari 90/95 thread che loopano su questo primo if (ancora il file non è stato creato) per molto tempo.
                            //Meglio metterli a dormire. Avrebbe molto senso, in questo specifico contesto, usare le coroutine e fare delle delay, così da venire sospesi
                            Thread.sleep(2000)
                            true
                        } else if (response.statusCode == HttpStatus.OK) {
                            if (validatedByte.toLong() != totalDimension) {
                                logger.trace("The file: ${file.key}  is not yet complete")
                                val result = validator.validate(response.body!!.data)
                                assertTrue(result) {
                                    "Validation failed. The retrieved content (dimension ${response.body!!.data.size}):  of the file: ${file.key} at offset: ${validatedByte} is not the same"
                                }
                                validatedByte += response.body!!.data.size

                                assertNotNull(response.body, "The response is null")
                                assertEquals(
                                    HttpStatus.OK,
                                    response.statusCode,
                                    "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                                )
                                assertEquals(file.key, response.body!!.file.filename, "The filename is not the same")
                                assertEquals(
                                    metadata["contentType"],
                                    response.body!!.file.metadata["_contentType"],
                                    "The field contentType of metadata is not the same"
                                )
                                //Return true if the file is not yet complete
                                if (validatedByte.toLong() != totalDimension) {
                                    true
                                } else {
                                    logger.info("The file: ${file.key}  is complete")
                                    false
                                }
                                //true
                            } else {
                                logger.info("The file: ${file.key}  is complete")
                                false
                            }
                        } else {
                            logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                            true
                        }
//                    Thread.sleep(1000)
                    } while (repeat)
                } catch (e: Exception) {
                    failedValidationFlag.set(true)
                    throw e
                }
            }
        }

        //Ogni volta, prima di mettermi in attesa del termine di un processo di validazione, controllo se qualcun altro è fallito
        //Così da sveltire il termine del test. Ad esempio se sto facendo la get in ordine per 1000 file e subito è fallita la validazione dell'ultimo, preferisco accorgermene in un tempo che al massimo dura la validazione di uno, non di 999
        for (file in files) {
            //Validation before, because I want to terminate as soon as a new validation assertion exception is thrown
            if (failedValidationFlag.get()){
                logger.error { "Validation is failed" }
                throw RuntimeException("Validation is failed")
            }
            try {
                validation[file.key]!!.get()
            }
            catch (e: Exception){
                logger.error("Error while validating the file ${file.key}. Error: ${e.message}")
                throw e
            }
        }
        for (file in files) {
            if(failedGenerationFlag.get()){
                logger.error { "Generation is failed" }
                throw RuntimeException("Generation is failed")
            }
            try {
                generation[file.key]!!.get()
            }
            catch (e: Exception){
                logger.error("Error while generating the file ${file.key}. Error: ${e.message}")
                throw e
            }
        }
    }
    //Non ho ancora trovato un modo di farlo funzionare. Sarebbe bello non mettere gli header e ottenere ogni volta la risposta completa fino a quel punto...
    //Ma forse rischio di esplodere dato che nel validator uso i bytearray e potrei ottenere una resource molto grande
    //Allora si potrebbero usare i range come nell'endpoint precedente ma non riesco... che sia un problema legato all'ultimo byte e a quella differenza di indici nel file service?
    @Test
    fun `WITH STREAM ENDPOINT write multiple file, calling generator multiple times, with value around the max size accepted by kafka, generating not so multi and multithread validation`(){
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val numberOfFiles = 10
        val maxDimension = 10*MEBIBYTE
        Generator.setFileMapProperty(numberOfFiles, maxDimension, fixedMaxDimension = true)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val files = Generator.getFiles()
        val threadRatio = 10

        //Genero in un thread a parte così comincio a validare prima di aver finito di generare
        //Problem with exception throws inside
        //thread (start = true) {
        val generation = mutableMapOf<String, Future<*>>()
        val nOfGeneratorWorker= (numberOfFiles/threadRatio).coerceAtMost(25).coerceAtLeast(15)
        val workerGenerator = Executors.newFixedThreadPool(nOfGeneratorWorker, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "generator-thread-${counter.incrementAndGet()}")
        })
        for (file in files) {
            generation[file.key] = workerGenerator.submit {
                println("I'm generating the file ${file.key}")
                val driverLocal = DriverImpl()
                val gen = Generator(file.key)
                var generatedDimension = 0L
                while (generatedDimension < file.value) {
                    var pieceDimension = Random.nextLong(maxRequestSize - 100L, (2 * maxRequestSize).toLong())
                    if (generatedDimension + pieceDimension > file.value) {
                        pieceDimension = file.value - generatedDimension
                    }
                    val content = gen.generate(pieceDimension)
                    driverLocal.writeOnDisk(
                        kafkaTemplate,
                        kafkaProperties.topics.listOfTopics[0],
                        file.key,
                        content,
                        metadata
                    )
                    generatedDimension += pieceDimension
                }
                println("Finish to send message for: ${file.key}")
            }
        }
        //}

        /*        //Create a map of validator
                val validators = mutableMapOf<String, Validator>()
                for (file in files){
                    validators[file.key] = Validator(file.key)
                }*/
        val nOfValidatorWorker = 200
        val workerValidator = Executors.newFixedThreadPool(nOfValidatorWorker, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "validator-thread-${counter.incrementAndGet()}")
        })
        val validation = mutableMapOf<String, Future<*>>()
        for (file in files){
            validation[file.key] = workerValidator.submit{
                println("I'm validating the file ${file.key}")
                var repeat: Boolean
                var response: ResponseEntity<Resource>
                var validatedByte = 0
                //Validate the content piece by piece
                val validator = Validator(file.key)
                val totalDimension = file.value
                Thread.sleep(20000)
                val requestedDimension = 20*MEBIBYTE
                while(validatedByte.toLong() < totalDimension){
                    val endRange = requestedDimension + validatedByte
                    val requestHeader = HttpHeaders().apply {
                        add(HttpHeaders.RANGE, "bytes=${validatedByte}-$endRange")
                    }
                    val expectedDimension = if (endRange > totalDimension) totalDimension - validatedByte else requestedDimension

                    val request = HttpEntity("", requestHeader)
                    response =restTemplate.exchange(
                        "/play/media/${file.key}",
                        HttpMethod.GET,
                        request
                    )

                    repeat = if (response.statusCode == HttpStatus.NOT_FOUND) {
                        logger.trace("The file: $filename is not yet created")
                        //Creando 100 file, con mono thread, i messaggi vengono mandati sequenzialmente per un unico file.
                        //Quindi lato client verranno consumati più velocemente (ho molti thread), in relazione ai messaggi ricevuti per differenti filename
                        //Quindi validando avrò magari 90/95 thread che loopano su questo primo if (ancora il file non è stato creato) per molto tempo.
                        //Meglio metterli a dormire. Avrebbe molto senso, in questo specifico contesto, usare le coroutine e fare delle delay, così da venire sospesi
                        Thread.sleep(2000)
                        true
                    } else if (response.statusCode == HttpStatus.OK || response.statusCode == HttpStatus.PARTIAL_CONTENT) {
                        if (validatedByte.toLong() != totalDimension) {
                            logger.trace("The file: $filename is not yet complete")
                            val retrievedDimension = response.headers.contentLength
                            assertEquals(
                                expectedDimension,
                                retrievedDimension,
                                "The retrieved content (dimension $retrievedDimension) of the file: ${file.key} at offset: ${validatedByte} has different size from the expected one"
                            )
                            var readDimension = 0
                            val result = validator.validate(response.body!!.inputStream.readNBytes(retrievedDimension.toInt()))
                            assertTrue(result) {
                                "Validation failed. The retrieved content (dimension $readDimension):  of the file: ${file.key} at offset: ${validatedByte} is not the same"
                            }
                            validatedByte += retrievedDimension.toInt()

                            assertNotNull(response.body, "The response is null")
/*                            assertEquals(
                                HttpStatus.OK,
                                response.statusCode,
                                "Status code is ${response.statusCode} and not ${HttpStatus.OK}"
                            )*/
                            //Return true if the file is not yet complete
                            validatedByte.toLong() != totalDimension
                            //true
                        } else {
                            logger.info("The file: $filename is complete")
                            false
                        }
                    } else {
                        logger.error("BAD REQUEST OR Other. The file is not yet complete: ${response.statusCode}")
                        true
                    }
//                    Thread.sleep(1000)
                }
            }
        }

        for (file in files) {
            generation[file.key]!!.get()
            validation[file.key]!!.get()
        }
    }


    @Autowired
    private lateinit var resourceLoader: ResourceLoader

    @Autowired
    lateinit var gridFs: GridFsTemplate

    @Test
    fun `streaming my video`(){
        val filename = "firstTest"
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("filename").`is`(filename)))
        assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")

        val requestHeader = HttpHeaders().apply {
            add(HttpHeaders.RANGE, "bytes=0-100")
        }

        val request = HttpEntity("", requestHeader)
//        val res = restTemplate.getForObject("/play/media/$filename", StreamingResponseBody::class.java)
        val response: ResponseEntity<Resource> =restTemplate.exchange(
            "/play/media/$filename",
            HttpMethod.GET,
//            request
        )
        response.body!!.contentLength()
        println("Finish! Length:${response.body!!.contentLength()}, name: ${response.body!!.filename}")

        val outputFileName = "retrievedFile.mp4"
        val output = FileOutputStream(outputFileName, false)

        val inputUri = resourceLoader.getResource("classpath:myVideo.mp4").uri
        val retrievedResFile = gridFs.getResource(filename)


        retrievedResFile.content.transferTo(output)
        val result = Files.mismatch(Path.of(inputUri), Path(outputFileName))

        assertEquals(result, -1, "Content doesn't match")
    }

}