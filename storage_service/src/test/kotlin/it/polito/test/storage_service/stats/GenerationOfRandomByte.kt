package it.polito.test.storage_service.stats

import it.polito.test.storage_service.client.generator.Generator
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import kotlin.random.Random

@SpringBootTest
class GenerationOfRandomByte {
    val filename = Random.nextLong().toString()
    @Test
    fun `generate 1 byte`(){
        val generator = Generator(filename)
        val timeBefore = System.currentTimeMillis()
        val oneByte = generator.generate(1)
        val timeAfter = System.currentTimeMillis()
        println("Time to generate 1 byte random number: ${timeAfter - timeBefore} ms")
    }
    @Test
    fun `generate 1G random byte`(){
        val generator = Generator(filename)
        val oneG = 1024 * 1024 * 1024
        val timeBefore = System.currentTimeMillis()
        for(i in 1..oneG) {
            val oneByte = generator.generate(1)
        }
        val timeAfter = System.currentTimeMillis()
        println("Time to generate 1GB of random number: ${timeAfter - timeBefore} ms")
    }
    //Equivalent because internally the function call n times nextLong
    @Test
    fun `generate max byteArray`(){
        val generator = Generator(filename)
        val maxDimensionForByteArray = Generator.maxDimension()
        val timeBefore = System.currentTimeMillis()
        val byteArrayMaxDim = generator.generate(maxDimensionForByteArray)
        val timeAfter = System.currentTimeMillis()
        println("Time to generate 1GB of random number: ${timeAfter - timeBefore} ms")
    }
}