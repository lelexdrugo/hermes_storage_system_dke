package it.polito.test.storage_service.functionality.gridFsCompatibility

import it.polito.test.storage_service.documents.FileDocument
import it.polito.test.storage_service.repositories.FileRepository
import mu.KotlinLogging.logger
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.InputStream

@SpringBootTest
@Testcontainers
//Disable the auto-configuration for an embedded MongoDB
//@DataMongoTest(excludeAutoConfiguration = [EmbeddedMongoAutoConfiguration::class])
class FileGridCompatibilityTest {
    companion object{
        @Container
        var container: MongoDBContainer = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
            //.withEnv("MONGO_INITDB_DATABASE", "admin")
            //.withEnv("MONGO_INITDB_ROOT_USERNAME", "Gabriele")
            //.withEnv("MONGO_INITDB_ROOT_PASSWORD", "test")
/*        var container: GenericContainer<*> = GenericContainer(DockerImageName.parse("mongo:5"))
            .withEnv("MONGO_INITDB_ROOT_USERNAME", "Gabriele")
            .withEnv("MONGO_INITDB_ROOT_PASSWORD", "test")
            .
            .withExposedPorts(27017)*/

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.mongodb.uri", container::getReplicaSetUrl)
            //registry.add("spring.data.mongodb.username", container::getReplicaSetUrl)
            //registry.add("spring.data.mongodb.password",  )
        }
    }
    private val logger = logger {}

    @Autowired
    private lateinit var fileRepo: FileRepository

/*    @Autowired
    lateinit var restTemplate: TestRestTemplate*/

    @Autowired
    lateinit var gridFs: GridFsTemplate

    @Autowired
    lateinit var mongoDB: MongoTemplate

    @AfterEach
    fun tearDownDb() {
        fileRepo.deleteAll()
    }

    @Test
    fun testcontainer() {
        //println("Param\nPort:\t ${(container::getExposedPorts)()}\tHost: ${(container::getHost)()}")
        logger.info("Db:\t${mongoDB.db.name}\tCollections:\t${mongoDB.collectionNames}")
        assertEquals(1,1, "Testcontainer doesn't work")
    }

    @Test
    fun `create an empty file with gridFS`() {
        val fileContent = ""
        val filename = "testFile"
        val inputStream: InputStream = fileContent.byteInputStream()
        val savedId = gridFs.store(inputStream, filename, "document/docx")
        val gridFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedId)))
        //logger.info("Db:\t${mongoDB.db.name}\tCollections:\t${mongoDB.collectionNames}")
        assertEquals(gridFile.filename, filename, "Filename doesn't match")
    }

    @Test
    fun `create a file with repo`() {
        val filename = "testFile"
        val savedFile: FileDocument = fileRepo.save(FileDocument().apply {
            this.filename = filename
        })
        val retrievedFile = fileRepo.findBy_id(savedFile._id)
        assertEquals(savedFile.filename, retrievedFile.filename, "Filename doesn't match")
    }

    @Test
    fun `create a file with repo and retrieve it with gridFS`() {
        val filename = "testFile"
        val savedFile: FileDocument = fileRepo.save(FileDocument().apply {
                this.filename = filename
            })
        //logger.info("Saved: $savedFile")
        val retrievedFile = gridFs.findOne(Query(Criteria.where("_id").`is`(savedFile._id)))
        //logger.info("Retrieved: $retrievedFile")

        assertEquals(savedFile.filename, retrievedFile.filename, "Filename doesn't match")
    }

    @Test
    fun `create a file with gridFS and retrieve it with repo`() {
        val fileContent = ""
        val filename = "testFile"
        val inputStream: InputStream = fileContent.byteInputStream()
        val savedId = gridFs.store(inputStream, filename, "document/docx")
        //logger.info("Saved: $savedId")
        val repoFile = fileRepo.findBy_id(savedId)
        logger.info("Retrieved:$repoFile")
        assertEquals(repoFile.filename, filename, "Filename doesn't match")
    }

}