package it.polito.client_producer

import it.polito.client_producer.configurations.KafkaProperties
import it.polito.client_producer.driver.DriverImpl
import it.polito.client_producer.generator.Generator
import it.polito.client_producer.generator.MEBIBYTE
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Profile
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.messaging.Message
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.properties.Delegates
import kotlin.random.Random

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties::class)
class ClientProducerApplication

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ClientProducerApplication::class.java)
    logger.info("STARTING THE APPLICATION")
    runApplication<ClientProducerApplication>(*args)
    logger.info("APPLICATION FINISHED")
}

@Profile("!test")
@Component
class Console : CommandLineRunner {
    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, Message<Any>>
    private val driver = DriverImpl()

    private val topic = "fileStorage"

    //instantiate kotlin logger
    private val logger = LoggerFactory.getLogger(Console::class.java)

    /**
     * single filename dimension
     * useFileMap [numberOfFile] [maxFileDimension]
     */
    override fun run(vararg args: String?) {
        logger.info("EXECUTING : command line runner")
        if(args.isEmpty()){
            logger.error("Missing arguments")
            return
        }
        when(args[0]){
            "single" ->{
                singleFileProducer(*args)
            }
            "useFileMap" ->{
                multipleFileProducer(*args)
            }
            else->{
                logger.error("Args0 is : ${args[0]}")
            }

/*            "test" ->{
                //val k = driver.helperKafkaTemplate()
                driver.writeOnDisk(kafkaTemplate, "test", "hello there", byteArrayOf(), mutableMapOf(),"pp")
            }*/
        }

        /*        val CHUNK_SIZE = 261120

                val CONTENT_SIZE = 10 * CHUNK_SIZE
                val MAX_REQUEST_SIZE = 1048576
                val contentByteArray = ByteArray(100000).also { Random().nextBytes(it) }*/
    }

    //Modified to not have OOM
    fun singleFileProducer(vararg args: String?) {
        if(args.size<3){
            logger.error("Missing arguments")
            return
        }
        lateinit var filename: String
        var dimension by Delegates.notNull<Long>()
        for (i in args.indices) {
            when (i) {
                0 -> {
                    logger.info("args[{}] - mode: {}", i, args[i])
                }
                1 -> {
                    logger.info("args[{}] - filename: {}", i, args[i])
                    filename = args[i].toString()
                }

                2 -> {
                    logger.info("args[{}] - dimension: {}", i, args[i])
                    dimension = args[i]!!.toLong()
                }

                else -> {
                    logger.info("args[{}] - other parameter: {}", i, args[i])
                }
            }
        }

        println("I'm generating the file ${filename}")
        val generator = Generator(filename)
        val metadata = mutableMapOf("contentType" to "document/docx")
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int

        var generatedDimension = 0L
        while (generatedDimension < dimension) {
            var pieceDimension = Random.nextLong((0.5 * maxRequestSize).toLong(), (1.1 * maxRequestSize).toLong())
            if (generatedDimension + pieceDimension > dimension) {
                pieceDimension = dimension - generatedDimension
            }
            val content = generator.generate(pieceDimension)
            generatedDimension += pieceDimension
            if(generatedDimension>=dimension){
                driver.writeOnDiskWithStats(
                    kafkaTemplate,
                    topic,
                    filename,
                    content,
                    metadata
                )
            }
            else {
                driver.writeOnDisk(
                    kafkaTemplate,
                    topic,
                    filename,
                    content,
                    metadata
                )
            }
        }
        println("Finish to send message for: ${filename}")
    }
    
    fun multipleFileProducer(vararg args: String?){
        /** Custom map setting **/
        var maxDimension: Long = -1
        if(args.size>1) {
            if (args[1] != null && args[1]!!.toInt() > 0) {
                val numberOfFiles = args[1]!!.toInt()
                    if (args.size>2 && args[2] != null && args[2]!!.toLong() > 0) {
                        maxDimension = args[2]!!.toLong()
                        Generator.setFileMapProperty(numberOfFiles, maxDimension, true)
                    }
                    else {
                        Generator.setFileMapProperty(numberOfFiles)
                    }
                val files = Generator.getFiles()
                println("Number of file to generate: ${files.size}")
                val timeBefore = System.currentTimeMillis()
                val nUsedThread = sendMessageMultiThread()
                val timeAfter = System.currentTimeMillis()
                println("Time to send all the messages: ${timeAfter - timeBefore} ms"+"\n" +
                        "Number of files: $numberOfFiles, maxDimension: $maxDimension (-1 is unspecified)"+" \n" +
                        "Number of thread: $nUsedThread")
            }
        }
    }

    @Value("\${number-of-generator-threads}")
    var numberOfGeneratorThreads: Int = 0
    @Value("\${sending-bytes-rate}")
    var sendingBytesRate: Int = 0
    fun sendMessageMultiThread(): Int{
        val maxRequestSize = kafkaTemplate.producerFactory.configurationProperties["max.request.size"] as Int
        val files = Generator.getFiles()
        val numberOfFiles = files.size
        val metadata = mutableMapOf("contentType" to "document/docx")
        val threadRatio = 10
        //Try to force a bit
        val nOfThread: Int = numberOfGeneratorThreads //20.coerceAtMost(numberOfFiles)// = numberOfFiles/threadRatio //Try this value after some tuning//nOfThread.coerceAtLeast(10).coerceAtMost(17)
        val worker = Executors.newFixedThreadPool(nOfThread, object : ThreadFactory {
            private val counter = AtomicInteger(0)
            override fun newThread(r: Runnable): Thread =
                Thread(null, r, "generator-thread-${counter.incrementAndGet()}")
        })

        //Array of futures
        val results: MutableList<Future<*>> = mutableListOf()
//        val timeBefore = System.currentTimeMillis()
        for (file in files) {
            val futureResult = worker.submit {
                println("I'm generating the file ${file.key} with kafkaTemplate: $kafkaTemplate")
                val driverLocal = DriverImpl()
                val gen = Generator(file.key)
                var generatedDimension = 0L
                var bytesSentInARow = 0L
                var stopForByteRate = false
                var startingTime= System.currentTimeMillis()
                var sleepingTime = 0L
                while (generatedDimension < file.value) {
//                        var pieceDimension = Random.nextLong(maxRequestSize - 100L, (2 * maxRequestSize).toLong())
                    var pieceDimension = Random.nextLong((0.5 * maxRequestSize).toLong(), (1.1 * maxRequestSize).toLong())
                    if (bytesSentInARow + pieceDimension >= sendingBytesRate*MEBIBYTE) {
                        //We reach the bytes desidered for the rate
                        //Check if we have to sleep
                        var timeToSendInARow=System.currentTimeMillis()
                        var elapsedTime=timeToSendInARow-startingTime
                        sleepingTime= 1000 - elapsedTime
                        if(sleepingTime>=0){
                            //We have to sleep
                            pieceDimension = sendingBytesRate * MEBIBYTE - bytesSentInARow  //This could be 0
                            stopForByteRate = true
                            bytesSentInARow=0
                        }
                        else{
                            stopForByteRate = false
                            logger.info("${Thread.currentThread().id} is sending data for file ${file.key} at a lower speed rate than $sendingBytesRate MiB/s (actual speed: ${"%.2f".format(((bytesSentInARow + pieceDimension)/MEBIBYTE)/(elapsedTime/1000.0))} MiB/s)")
                            bytesSentInARow=0
                            startingTime=System.currentTimeMillis()
                        }

                    }

                    if (generatedDimension + pieceDimension > file.value) {
                        pieceDimension = file.value - generatedDimension
                    }
                    if(pieceDimension>0){
                        val content = gen.generate(pieceDimension)
                        generatedDimension += pieceDimension
                        bytesSentInARow += pieceDimension
                        if(generatedDimension>=file.value){
                            driverLocal.writeOnDiskWithStats(
                                kafkaTemplate,
                                topic,
                                file.key,
                                content,
                                metadata
                            )
                        }
                        else {
                            driverLocal.writeOnDisk(
                                kafkaTemplate,
                                topic,
                                file.key,
                                content,
                                metadata
                            )
                        }
                    }

                    if(stopForByteRate){
                        Thread.sleep(sleepingTime)
                            logger.info("Thread ${Thread.currentThread().id} going to sleep for $sleepingTime ms to respect sending rate of $sendingBytesRate MiB/s for file ${file.key}")
                        startingTime=System.currentTimeMillis()
                        bytesSentInARow=0
                        stopForByteRate=false
                    }
                }
                println("Finish to send message for: ${file.key}")
                if(kafkaTemplate.producerFactory.isProducerPerThread){
                    kafkaTemplate.producerFactory.closeThreadBoundProducer()
                }
            }
            results.add(futureResult)
        }
        for (future in results) {
            future.get()
        }
//        val timeAfter = System.currentTimeMillis()
        worker.shutdown() //...
        return nOfThread
    }

}

