package it.polito.test.storage_service.utilities


import it.polito.test.storage_service.client.driver.DriverImpl
import it.polito.test.storage_service.client.generator.Generator
import it.polito.test.storage_service.client.generator.MEBIBYTE
import it.polito.test.storage_service.client.validator.Validator
import it.polito.test.storage_service.configurations.KafkaProperties
import it.polito.test.storage_service.dtos.FileResourceBlockDTO
import it.polito.test.storage_service.services.FileService
import mu.KotlinLogging
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.client.exchange
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.context.annotation.Bean
import org.springframework.core.io.*
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.http.*
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestTemplate
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.*
import java.nio.file.Path
import kotlin.random.Random


@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Utilities {
    companion object{
/*                @Container
                var mongoContainer: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
                @Container
                var kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1")).apply {
                    this.start()
                }*/

//        var mongoContainerReplicated = MongoContainer().apply { this.use() }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
/*            registry.add("spring.data.mongodb.uri", mongoContainer::getReplicaSetUrl)
//            registry.add("spring.data.mongodb.uri", mongoContainerReplicated::getReplicaSetUrl)
            registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers)*/
        }
    }

    @Autowired
    private lateinit var resourceLoader: ResourceLoader

    @Autowired
    lateinit var gridFs: GridFsTemplate

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
        //mongoDb.db.getCollection("rcs.files").drop()
        //mongoDb.db.getCollection("rcs.chunks").drop()
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
    fun `inject my video with gridFS`() {
        val filename = "hugeFileForTest"
        val inputUri = resourceLoader.getResource("classpath:hugeVideo.mp4").uri
        val inputStream = File(inputUri).inputStream()
        val timeBefore = System.currentTimeMillis()
        val savedId = gridFs.store(inputStream, filename, "video/mp4")
        println("Time to store the file: ${System.currentTimeMillis() - timeBefore}")
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
//        val retrievedResFile = gridFs.getResource(filename)
        assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
/*
        val outputFileName = "retrievedFile.jpg"
        val output = FileOutputStream(outputFileName, false)
        retrievedResFile.content.transferTo(output)
        val result = Files.mismatch(Path.of(inputUri), Path(outputFileName))

        assertEquals(result, -1, "Content doesn't match")*/
    }
    @Test
    fun `inject a lot of file with gridFS`() {
        val filename = "hugeFileForTest"
        val inputUri = resourceLoader.getResource("classpath:hugeVideo.mp4").uri
        val inputStream = File(inputUri).inputStream()
        val timeBefore = System.currentTimeMillis()
        var counter =0
        while(inputStream.available() > 1){
            val byte = inputStream.readNBytes((MEBIBYTE/4).toInt())
            gridFs.store(byte.inputStream(), filename, "video/mp4")
            counter++
        }

        println("Time to store $counter file: ${System.currentTimeMillis() - timeBefore}")
    }

/*    @Autowired
    var environment: Environment? = null

    var port: String? = environment?.getProperty("local.server.port")*/
    @LocalServerPort
    var port: Int? = null

    @Test
    fun `upload a file`() {
        //In a post parameter are in the body. In this case following the multipart/form-data format
/*        val headers = HttpHeaders()
        val requestCallback = RequestCallback { request: ClientHttpRequest ->
            request.headers.contentType = MediaType.MULTIPART_FORM_DATA
            val parameters = LinkedMultiValueMap<String, Any>()
            parameters.add("file", ClassPathResource("bigVideo2.mp4"))
            parameters.add("name", "beautifulMediumVideo")
            request.body(parameters.toSingleValueMap())
            IOUtils.copy(inputBodyStreamResource.getInputStream(), request.body)
        }*/
        //val myrs= myrsBuilder.setBufferRequestBody(false).build()
        logger.info { "port: $port" }
        val restTemplate = RestTemplate()

        val requestFactory = SimpleClientHttpRequestFactory()
        requestFactory.setBufferRequestBody(false)
        restTemplate.requestFactory = requestFactory

        val parameters = LinkedMultiValueMap<String, Any>()
/*        val inputUri = resourceLoader.getResource("classpath:hugeVideo.mp4").uri
        val inputRes = FileSystemResource(Path.of("src", "test", "resources", "hugeVideo.mp4"))
        val input= InputStreamResource(inputRes.inputStream)*/
        parameters.add("file", ClassPathResource("myVideo.mp4"))
//        parameters.add("file", input)
        parameters.add("name", "beautifulMediumVideo3")
        val headers = HttpHeaders()
        headers.contentType = MediaType.MULTIPART_FORM_DATA

        val entity = HttpEntity(parameters, headers)
        val response: ResponseEntity<String> = restTemplate.exchange(
            "http://localhost:$port/video", HttpMethod.POST, entity,
            String::class.java, ""
        )

        assertEquals(response.statusCode, HttpStatus.OK)
    }

    @Autowired
    lateinit var myrsBuilder: RestTemplateBuilder

    @Bean
    fun restTemplate(): RestTemplate? {
        val rf = SimpleClientHttpRequestFactory()
        rf.setBufferRequestBody(false)
        return RestTemplate(rf)
    }

    @Test
    fun `streaming my video`(){
        val filename = "hugeFileForTest"
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

        println("Finish! Length of response body:${response.body!!.contentLength()}, name: ${response.body!!.filename}")
        println("From header content length: ${response.headers.contentLength}")

/*        val outputFileName = "retrievedFile.mp4"
        val output = FileOutputStream(outputFileName, false)

        val inputUri = resourceLoader.getResource("classpath:myVideo.mp4").uri
        val retrievedResFile = gridFs.getResource(filename)


        retrievedResFile.content.transferTo(output)
        val result = Files.mismatch(Path.of(inputUri), Path(outputFileName))

        assertEquals(result, -1, "Content doesn't match")*/
    }

    @Autowired
    lateinit var fileService: FileService

    @Test
    fun `inject my video with function chain`() {
        val filename = "DiNuovoHuge4"
//        val res= FileSystemResource(Path.of("src", "test", "resources", "hugeVideo.mp4"))
        val res= FileSystemResource(Path.of("src", "test", "resources", "hugeVideoatomAhead.mp4"))
        val inputStream = res.inputStream
        //Brutto, ma non voglio riscrivere la catena
//        val seq = SequenceInputStream(inputStream, InputStream.nullInputStream())
        var buffer = ByteArray((0.5*MEBIBYTE.toInt()).toInt())
        val read= inputStream.read(buffer)
        val timeBefore = System.currentTimeMillis()
        fileService.createFile(filename, buffer.inputStream(), "video/mp4", -1, read.toLong())
        var offset = 0
        while (inputStream.available() > 1){
            val file = fileService.getFileEntity(filename)!!
            val read = inputStream.read(buffer)
            fileService.updateFile(file, buffer.inputStream(), offset.toLong(), read.toLong())
            offset++
        }
        inputStream.close()
        println("Creato!")
        println("Time to store the file: ${System.currentTimeMillis() - timeBefore}")
        val file = fileService.getFileEntity(filename)!!
        val retrievedGridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(file._id)))
//        val retrievedResFile = gridFs.getResource(filename)
        assertEquals(retrievedGridFile.filename, filename, "Filename doesn't match")
        /*
                val outputFileName = "retrievedFile.jpg"
                val output = FileOutputStream(outputFileName, false)
                retrievedResFile.content.transferTo(output)
                val result = Files.mismatch(Path.of(inputUri), Path(outputFileName))

                assertEquals(result, -1, "Content doesn't match")*/
    }

    @Test
    fun `test sequence work`(){
        val mapName = mutableMapOf<Int,String>()
        for (i in 0..100){
            mapName[i] = "name$i"
        }
        try {

            mapName.asSequence().forEach { (key, value) ->
/*                if(key == 10)
                    throw Exception()*/
/*                if(key == 20)
                    return*/
                if(key == 50)
                    return@forEach
                println("key: $key, value: $value") }
        }
        catch (e: Exception){
            println("Out of forEach in catch")
        }
        println("Out of forEach")
    }

    @Test
    fun `test concat sequenceInputStream`(){
        val byte1 = byteArrayOf(1,2,3)
        val byte2 = byteArrayOf(4,5,6)
        val byte3 = byteArrayOf(7,8,9)
        val byte4 = byteArrayOf(10,11,12)

        val saA = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val saB = SequenceInputStream(byte3.inputStream(), byte4.inputStream())

        val saC = SequenceInputStream(saA, saB)
        val ba = ByteArray(12)
        saC.readNBytes(ba, 0, 12)
        println("Concat: ${ba.toList()}")

        val sa1 = SequenceInputStream(byte1.inputStream(), InputStream.nullInputStream())
        val sa2 = SequenceInputStream(byte2.inputStream(), InputStream.nullInputStream())
        val sa3 = SequenceInputStream(byte3.inputStream(), InputStream.nullInputStream())
        val sa4 = SequenceInputStream(byte4.inputStream(), InputStream.nullInputStream())

        val saComplessivo1 = SequenceInputStream(sa1, sa2)
        val saComplessivo2 = SequenceInputStream(saComplessivo1, sa3)
        val saComplessivo4 = SequenceInputStream(saComplessivo2, sa4)

        val baComplessivo = ByteArray(12)
        saComplessivo4.readNBytes(baComplessivo, 0, 12)
        println("Complessivo: ${baComplessivo.toList()}")
    }

    @Test
    fun `test skip sequence input stream`(){
        //Skip funziona bene... torna meno byte solo se lo stream finisce completamente.
        val byte1 = byteArrayOf(1,2,3)
        val byte2 = byteArrayOf(4,5,6)
        val sa1 = SequenceInputStream(byte1.inputStream(), InputStream.nullInputStream())
        val sa2 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())

        sa1.skip(2)
        val ba1 = ByteArray(6)
        sa1.readNBytes(ba1, 0, 6)
        println("First skipping 2: ${ba1.toList()}")

        sa2.skip(2)
        val ba2 = ByteArray(6)
        sa2.readNBytes(ba2, 0, 6)
        println("Second skipping 2: ${ba2.toList()}")
        val sa3 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val resSkip3 = sa3.skip(4)
        println("Res skip 3: $resSkip3")
        val ba3 = ByteArray(6)
        sa3.readNBytes(ba3, 0, 6)
        println("Third skipping 4: ${ba3.toList()}")

        val sa4 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val ba4 = ByteArray(6)
        val res = sa4.read(ba4, 0, 4)
        println("Res: $res")
        println("Fourth reading 4: ${ba4.toList()}")

        val sa5 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val ba5 = ByteArray(6)
        val res2 = sa5.read(ba5, 0, 2)
        println("Res2: $res2")
        println("Fifth reading 3: ${ba5.toList()}")
        val skippingRes = sa5.skip(2)
        println("Skipping res: $skippingRes")
        val ba6 = ByteArray(6)
        val res3 = sa5.read(ba6, 0, 6)
        println("Res3: $res3")
        println("Sixth reading 6: ${ba6.toList()}")

        val ba7 = ByteArray(6)
        val sa7 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        sa7.skipNBytes(4)
        val res4 = sa7.read(ba7, 0, 6)
        println("Res4: $res4")
        println("Seventh reading 6: ${ba7.toList()}")


    }

    @Test
    fun `test read sequence input stream`(){
        //Se voglio compatiblità tra InputStream e SequenceInputStream, devo usare dei metodi che in fondo chiamino la read della Sequence e che controllino il valore di ritorno.
        //Sbagliato usare available.
        //Nota sul polimorfismo: ricorda la vtable. Una variabile anche se passata a un metodo che la usa come InputStream, se è una SequenceInputStream, usa la read della SequenceInputStream.
        //Sbagliato usare la funzione read(byte[]), che chiama read(byte[], offset, len), così come la read(byte[],offset,len) di sequence che a sua volta richiama quella di inputStream.
        // Questo perché non cicla fino alla lettura di N bytes. Se riesce a leggere un po' ritorna quelli.
        //Ma readNBytes di InputStream è una funzione che cicla fino a leggere N bytes. Quindi funziona, inutile riscriverla

        val byte1 = byteArrayOf(1,2,3)
        val byte2 = byteArrayOf(4,5,6)
        val sa1 = SequenceInputStream(byte1.inputStream(), InputStream.nullInputStream())
        val sa2 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        while (sa1.available() > 0){
            println(sa1.read())
        }
        println("Second based on available")
        while (sa2.available() > 0){
            println(sa2.read())
        }
        println("Second forced on 6")
        var i = 6
        val sa3 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        while (i > 0){
            println(sa3.read())
            i--
        }

        val sa4 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())

        val result = ByteArray(6)
        sa4.read(result)
        println("Simple read by inputStream: ${result.toList()}")

        val sa5 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val result2 = ByteArray(6)
        sa5.read(result2, 0, 6)
        println("Read array with offset, overwritten from inputStream: ${result2.toList()}")

        val sa6 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        readOffset(sa6)

        val sa7 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val result3 = ByteArray(6)
        myRead(sa7, result3, 0, 6)

        val sa8 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val result4 = ByteArray(6)
        myReadWithDoWhile(sa8, result4, 0, 6)

        val sa9 = SequenceInputStream(byte1.inputStream(), byte2.inputStream())
        val result5 = ByteArray(6)
        sa9.readNBytes(result5, 0, 6)
        println("ReadNBytes: ${result5.toList()}")
    }

    fun myRead(inputStream: InputStream, buffer: ByteArray, offset: Int, length: Int): Int{
        var read = 0
        var totalRead = 0
        while (totalRead < length){
            read = inputStream.read(buffer, offset + totalRead, length - totalRead)
            if(read == -1)
                break
            totalRead += read
        }
        println("Read with myRead: ${buffer.toList()}")
        return totalRead
    }

    fun myReadWithDoWhile(inputStream: InputStream, buffer: ByteArray, offset: Int, length: Int): Int{
        var read = 0
        var totalRead = 0
        do {
            read = inputStream.read(buffer, offset + totalRead, length - totalRead)
            if(read == -1)
                break
            totalRead += read
        } while (totalRead < length)
        println("Read with myReadDoWHile: ${buffer.toList()}")
        return totalRead
    }

    fun readOffset(inputStream: InputStream){
        val buffer = ByteArray(6)
        val read = inputStream.read(buffer, 0 , 6)
        println("Read with readOffset (same of before, thanks to polymorphism): ${buffer.toList()}")
    }
}