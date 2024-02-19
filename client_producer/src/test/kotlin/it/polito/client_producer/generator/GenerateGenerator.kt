package it.polito.client_producer.generator

import it.polito.client_producer.Console
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test") //Needed to avoid the execution of the component CommandLineRunner in the application
@SpringBootTest
class GenerateGenerator {

    val console: Console = Console()
    //val client = ClientProducerApplication()

    //write test that run main with args
    @Test
    fun main() {
        console.run("single", "FileDiProvaDaTestare.txt", "100000")
        //val g = Generator("ciao")
        //println("g: $g")
    }
}