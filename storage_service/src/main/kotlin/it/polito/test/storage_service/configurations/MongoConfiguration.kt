package it.polito.test.storage_service.configurations

import com.mongodb.*
import com.mongodb.connection.ClusterSettings
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import org.springframework.data.mongodb.gridfs.GridFsTemplate
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories


/**
 * To use gridFS in spring I must provide a configuration class.
 * So I have to specify here custom client connection (with username and password)
 * Before it was created automatically
 */
@Configuration
@EnableMongoRepositories(basePackages = ["it.polito.test.storage_service.repositories"])
class MongoConfiguration(private val mongoProp: MongoProperties,
                         @org.springframework.context.annotation.Lazy private val mongoConverter: MappingMongoConverter) : AbstractMongoClientConfiguration() {
/*    @Autowired
    private var context: ApplicationContext? = null*/

/*    @Bean
    override fun mongoTemplate(databaseFactory: MongoDatabaseFactory, mongoConverter: MappingMongoConverter): MongoTemplate {
        val mongoTemplate = MongoTemplate(mongoDbFactory(), mongoConverter);
        // Version 1: set statically
        //logger.debug("Setting WriteConcern statically to ACKNOWLEDGED");
        mongoTemplate.setWriteConcern(WriteConcern.MAJORITY);
        return mongoTemplate;
    }*/
/*    @Bean
    fun writeConcernResolver(): WriteConcernResolver? {
        return WriteConcernResolver { action: MongoAction? ->
            println("Using Write Concern of Acknowledged")
            WriteConcern.MAJORITY
        }
    }*/

    //To create index with annotation
    override fun autoIndexCreation(): Boolean {
        return true
    }
    @Bean
    @Throws(java.lang.Exception::class)
    fun gridFsTemplate(): GridFsTemplate {
        return GridFsTemplate(mongoDbFactory(), mongoConverter, mongoProp.gridFs.bucket)
        //return GridFsTemplate(mongoDbFactory(), mappingMongoConverter(mongoDbFactory(), customConversions(), mongoMappingContext(customConversions())), "rcs")
        //return GridFsTemplate(mongoDbFactory(),   context?.getBean(MappingMongoConverter::class.java)!!, "rcs")
    }


    override fun getDatabaseName(): String {
        return mongoProp.database
    }

    override fun configureClientSettings(builder: MongoClientSettings.Builder) {
        println("MONGODB uri in configure: ${mongoProp.uri}")
        println("Db name in configure: $databaseName")
         //è strano: osservando i metodi interni di mongo sembra costruire correttamente le credential a partire dall'uri, ma poi non le utilizza
        //if(mongoProp.uri.isEmpty())
        //Use old connection method that not support replica set.
         val connectionString = ConnectionString(mongoProp.uri)
         builder
             .writeConcern(
                 WriteConcern.MAJORITY
                    //WriteConcern.W3
                     .withWTimeout(mongoProp.writeConcern.wtimeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
                     .withJournal(mongoProp.writeConcern.journal)
             )
             //.credential(MongoCredential.createCredential("gabriele", "admin", "test"))
             .applyToClusterSettings { settings: ClusterSettings.Builder ->
                 settings.applyConnectionString(connectionString)
             }
             .also{
                 if(connectionString.credential?.userName != null)
                    it.credential(connectionString.credential!!)}
         //standard uri connection schema
         //mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]
         //uri in the form: mongodb://username:password@host:port/database
         //mongodb://localhost:53709/test if no auth required
         //parse mongoProp.uri to extract field
       /*  var host: String
         var port :String
         lateinit var credential: MongoCredential
        val mongoUri = mongoProp.uri
        val mongoUriParts = mongoUri.split("@")
         //check if the Optional Authentication credential is present
         //if present try to authenticate to the specified authSource (database name)
         //if no authSource is specified try to connect to admin database
         if (mongoUriParts.size == 2) { //auth required
                val authPart = mongoUriParts[0].split("//")[1]
                val authPartParts = authPart.split(":")
                val username = authPartParts[0]
                val password = authPartParts[1]
             //Check if the uri is the one of a replica set connection
             //mongodb://mongo1:27017,mongo2:27017,mongo3:27017
             //Split the uri by comma and check if the size is greater than 1. Then loop on it separating host and port creating a list of ServerAddress
                //If the uri is not a replica set connection then just create a ServerAddress
//...

                host = mongoUriParts[1].split(":")[0]
                port = mongoUriParts[1].split(":")[1].split("/")[0]
                val databasePartParts = mongoUriParts[1].split("/")
                val database = if (databasePartParts.size >= 2) databasePartParts[1] else "admin"
                println("MONGODB username: $username")
//                println("MONGODB password: $password")
                println("MONGODB password: *****")
                println("MONGODB host: $host")
                println("MONGODB port: $port")
                println("MONGODB database: $database")
                credential = MongoCredential.createCredential(username, database, password.toCharArray())
            } else { //no auth required
             host = mongoUriParts[0].split("//")[1].split(":")[0]
             port = mongoUriParts[0].split("//")[1].split(":")[1].split("/")[0]
             val database = mongoUriParts[0].split("//")[1].split(":")[1].split("/")[1]
             println("MONGODB host: $host")
             println("MONGODB port: $port")
             println("MONGODB database: $database")
         }
         val serverAddress = ServerAddress(host, port.toInt())
         builder
                    .writeConcern(
                        //WriteConcern(mongoProp.writeConcern)
                        WriteConcern.MAJORITY
                            .withWTimeout(1000, java.util.concurrent.TimeUnit.MILLISECONDS)
                            .withJournal(true)
                    )
                        .applyToClusterSettings { settings: ClusterSettings.Builder ->
                    settings.hosts(
                        listOf(serverAddress)
                    )
                }
         if (mongoUriParts.size == 2) {
                builder.credential(credential)
         }*/
/*        builder
            .credential(MongoCredential.createCredential(mongoProp.username, mongoProp.database, mongoProp.password.toCharArray()))
            .applyToClusterSettings { settings: ClusterSettings.Builder ->
                settings.hosts(
                    listOf(ServerAddress(mongoProp.host, mongoProp.port.toInt()))
                )
            }*/
    }
}