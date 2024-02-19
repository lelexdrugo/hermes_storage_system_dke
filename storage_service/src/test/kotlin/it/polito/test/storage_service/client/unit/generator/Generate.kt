package it.polito.test.storage_service.client.unit.generator

import it.polito.test.storage_service.client.generator.Generator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import kotlin.random.Random

@SpringBootTest
class Generate {
    // test function generate of class Generator
    // instantiate a Generator object with a string
    // call the generate function
    // check that the result is a list of FileResourceBlockDTO
    //val filename: String = "Test"
    val filename: String = Random.nextLong().toString()
    val dimension: Long = 100L

    @Test
    fun `instantiate a generator and check that the filename used is the seed of the generator`() {
        val generator = Generator(filename)
        val actualResult = generator.generate(dimension)
        val testGenerator = Random(filename.hashCode())
        val expectedResult = testGenerator.nextBytes(dimension.toInt())
        Assertions.assertArrayEquals(expectedResult, actualResult) {
            "The generated content is $actualResult and not $expectedResult"
        }
    }

    @Test
    fun `instantiate a generator and check that a generator with a different filename return different values`(){
        val generator = Generator(filename)
        val actualResult = generator.generate(dimension)
        val testGenerator = Random("Test2".hashCode())
        val expectedResult = testGenerator.nextBytes(dimension.toInt())
        Assertions.assertNotEquals(expectedResult, actualResult) {
            "The generated content with a different generator is the same of: $actualResult"
        }
    }

    @Test
    fun `call generate more than one time on consecutive piece of content`(){
        val newGenerator = java.util.Random(filename.hashCode().toLong())

        val generator = Generator(filename)
        val totalDimension = 5*dimension
//        val testGenerator = Random(filename.hashCode())
//        val expectedResult = testGenerator.nextBytes(totalDimension.toInt())
        val expectedResult = ByteArray(totalDimension.toInt())
        newGenerator.nextBytes(expectedResult)
        for(i in 0 until 5){
            val actualResult = generator.generate(dimension)
            Assertions.assertArrayEquals(expectedResult.sliceArray(i*dimension.toInt() until (i+1)*dimension.toInt()), actualResult) {
                "The generated content is $actualResult and not ${expectedResult.sliceArray(i*dimension.toInt() until (i+1)*dimension.toInt())}"
            }
        }
    }

    @Test
    //test function generate of class Generator with a dimension of 0
    fun `call generate with a dimension of 0`(){
        val generator = Generator(filename)
        val actualResult = generator.generate(0)
        val expectedResult = ByteArray(0)
        Assertions.assertArrayEquals(expectedResult, actualResult) {
            "The generated content is $actualResult and not $expectedResult"
        }
    }
    @Test
    fun `call generate with a dimension of 0 and then compare content with a dimension of 100`(){
        println("filename: $filename")
        val generator = Generator(filename)
        val actualResult = generator.generate(0)
        val expectedResult = ByteArray(0)
        Assertions.assertArrayEquals(expectedResult, actualResult) {
            "The generated content is $actualResult and not $expectedResult"
        }
        val actualResult2 = generator.generate(dimension)
//        val testGenerator = Random(filename.hashCode())
//        val expectedResult2 = testGenerator.nextBytes(dimension.toInt())
        val newGenerator = java.util.Random(filename.hashCode().toLong())
        val expectedResult2 = ByteArray(dimension.toInt())
        newGenerator.nextBytes(expectedResult2)

        Assertions.assertArrayEquals(expectedResult2, actualResult2) {
            "The generated content is $actualResult2 and not $expectedResult2"
        }
    }

    @Test
    fun `call generate with a dimension of 0 more than one time and then compare content with a dimension of 100`() {
        val generator = Generator(filename)
        val actualResult = generator.generate(0)
        val expectedResult = ByteArray(0)
        Assertions.assertArrayEquals(expectedResult, actualResult) {
            "The generated content is $actualResult and not $expectedResult"
        }
        val actualResult2 = generator.generate(0)
        val expectedResult2 = ByteArray(0)
        Assertions.assertArrayEquals(expectedResult2, actualResult2) {
            "The generated content is $actualResult2 and not $expectedResult2"
        }
        val actualResult3 = generator.generate(dimension)
        val newGenerator = java.util.Random(filename.hashCode().toLong())
        val expectedResult3 = ByteArray(dimension.toInt())
        newGenerator.nextBytes(expectedResult3)

        Assertions.assertArrayEquals(expectedResult3, actualResult3) {
            "The generated content is $actualResult3 and not $expectedResult3"
        }
    }

    /**
     * Test about max dimension
     */
    /********************************************/

    fun maxByteArraySize(): Int {
        var size = Int.MAX_VALUE
        while (--size > 0) try {
            ByteArray(size)
            break
        } catch (t: Throwable) {
            println("Size is too much: $size")
        }
        return size
    }
    fun maxByteArraySizeExp(): Int {
        var size = Int.MAX_VALUE
        while (size > 0) try {
            ByteArray(size)
            break
        } catch (t: Throwable) {
            println("Size is too much: $size")
            size /= 2
        }
        return size
    }
    fun maxByteArraySizeDec(): Int {
        var size = 536870911
        while (size > 0) try {
            ByteArray(size)
            break
        } catch (t: Throwable) {
            println("Size is too much: $size")
            size -= 100000
        }
        return size
    }

    @Test
    fun printMaxSize(){
        println("Max size is ${maxByteArraySizeDec()}")
    }



    /**
     * Of course these two test doesn't work. Generate function must work only with Int dimension
     */
    @Test
    fun `call generate with the dimension of max value of int`(){
        val generator = Generator(filename)
        val freeMemBefore = Runtime.getRuntime().freeMemory()
        println("free memory before: $freeMemBefore")
        val actualResult = generator.generate(Int.MAX_VALUE.toLong())
        val testGenerator = java.util.Random(filename.hashCode().toLong())
        val freeMemAfter = Runtime.getRuntime().freeMemory()
        println("free memory after: $freeMemAfter")
        val maxDim = Generator.maxDimension()
//        val expectedResult = ByteArray(maxDim.toInt())
        val expectedResult = ByteArray(freeMemAfter.toInt())
        testGenerator.nextBytes(expectedResult)
        Assertions.assertArrayEquals(expectedResult, actualResult) {
            "The generated content is $actualResult and not $expectedResult. Content is not trimmed"
        }
    }

    @Test
    fun `call generate with the dimension of max value of Long`(){
        val generator = Generator(filename)
        val actualResult = generator.generate(Long.MAX_VALUE)
        val testGenerator = java.util.Random(filename.hashCode().toLong())
        val expectedResult = ByteArray(Long.MAX_VALUE.toInt())
        testGenerator.nextBytes(expectedResult)
        Assertions.assertArrayEquals(expectedResult, actualResult) {
            "The generated content is $actualResult and not $expectedResult"
        }
    }
}