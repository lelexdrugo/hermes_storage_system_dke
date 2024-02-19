package it.polito.test.storage_service.mongoReplicaSetSupport

import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.Network.newNetwork
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName

//Alternative to testcontainer classic:
//https://github.com/silaev/mongodb-replica-set/
//add dependecy on gradle
class MongoContainer {
    companion object {
//        var INSTANCE = MongoContainer()
        private val MONGO_PORT = 27017
        private val MONGO_IMAGE = "mongo:4.4.2"
        private val m1_exposed_port = 30001
        private val m2_exposed_port = 30002
        private val m3_exposed_port = 30003

        val network: Network = newNetwork()
        //        val m1: GenericContainer<*> = GenericContainer(MONGO_IMAGE)
        //Come per il replica set nell'applicazione, devo usare lo stesso hostname per risolvere l'host all'interno del dns di windows
        //Non m1 ma mongo1...
        //Inoltre per lo stesso motivo devo esporre porte diverse, altrimenti sono nascosti tutti dietro uno (stesso ip -> localhost)
        @Container
        val m1 = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
            .withNetwork(network)
            .withNetworkAliases("mongo1")
            .withExposedPorts(m1_exposed_port)
//            .withExposedPorts(MONGO_PORT)
//            .withCommand("--replSet rs0 --bind_ip localhost,mongo1")
            .withCommand("--replSet rs0 --bind_ip localhost,mongo1")
        //--port 30002 --replSet rset --bind_ip 0.0.0.0
        //        val m2: GenericContainer<*> = GenericContainer(MONGO_IMAGE)
        @Container
        val m2 = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
            .withNetwork(network)
            .withNetworkAliases("mongo2")
            .withExposedPorts(m2_exposed_port)
            .withCommand("--replSet rs0 --bind_ip localhost,mongo2")
        //        val m3: GenericContainer<*> = GenericContainer(MONGO_IMAGE)
        @Container
        val m3 = MongoDBContainer(DockerImageName.parse("mongo:4:4:2"))
            .withNetwork(network)
            .withNetworkAliases("mongo3")
            .withExposedPorts(m3_exposed_port)
            .withCommand("--replSet rs0 --bind_ip localhost,mongo3")
        /*        m1.start()
                m2.start()
                m3.start()*/
        }


/*    private lateinit var mongoClient: MongoClient*/
    private lateinit var endpointURI: String

    fun use(){
        m1.start()
        m2.start()
        m3.start()
            try {
                m1.execInContainer(
                    "/bin/bash", "-c",
                    "mongo --eval 'printjson(rs.initiate({_id:\"rs0\","
                            + "members:[{_id:0,host:\"mongo1:${m1_exposed_port}\"},{_id:1,host:\"mongo2:${m2_exposed_port}\"},{_id:2,host:\"mongo3:${m3_exposed_port}\"}]}))' "
//                            + "members:[{_id:0,host:\"mongo1:27017\"},{_id:1,host:\"mongo2:27017\"},{_id:2,host:\"mongo3:27017\"}]}))' "
                            + "--quiet"
                )
                m1.execInContainer(
                    "/bin/bash", "-c", (
                            "until mongo --eval \"printjson(rs.isMaster())\" | grep ismaster | grep true > /dev/null 2>&1;"
                                    + "do sleep 1;done")
                )
            } catch (e: Exception) {
                throw IllegalStateException("Failed to initiate rs.", e)
            }
            endpointURI = "mongodb://" + "mongo1" /*m1.containerIpAddress*/ + ":" + m1_exposed_port +"," +
                    "mongo2" + ":" + m2_exposed_port +"," +
                    "mongo3" + ":" + m3_exposed_port +"/replicaSet=rs0"
/*            mongoClient = MongoClients
                .create(endpointURI)*/
        }

/*    fun getMongoClient(): MongoClient {
        return mongoClient
    }*/

    fun getReplicaSetUrl(): String {
        return endpointURI
    }
}