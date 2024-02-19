package it.polito.test.storage_service.client.unit.validator

import it.polito.test.storage_service.client.validator.Validator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import kotlin.random.Random

@SpringBootTest
class Validate {
//    val filename: String = "Test"
    val filename: String = Random.nextLong().toString()
    val dimension: Long = 100L

    @Test
    fun `instantiate a validator and check that the filename used is the seed of the generator`() {
        val validator = Validator(filename)
        val testRandomSource = Random(filename.hashCode())
        val testContent = testRandomSource.nextBytes(dimension.toInt())
        val result = validator.validate(testContent)
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
    }

    @Test
    fun `instantiate a validator and check that a validator with a different filename return different values`(){
        val validator = Validator(filename)
        val testRandomSource = Random("Test2".hashCode())
        val testContent = testRandomSource.nextBytes(dimension.toInt())
        val result = validator.validate(testContent)
        Assertions.assertFalse(result) {
            "The validator content used internally is the same of: $testContent"
        }
    }

    @Test
    fun `call validate more than one time on consecutive piece of content`(){
        val validator = Validator(filename)
//        val testRandomSource = Random(filename.hashCode())
        val newRandomSource = java.util.Random(filename.hashCode().toLong())
        val totalDimension = 5*dimension
//        val testContent = testRandomSource.nextBytes(totalDimension.toInt())
        val testContent = ByteArray(totalDimension.toInt())
        newRandomSource.nextBytes(testContent)

        val part1 = testContent.sliceArray(0 until dimension.toInt())
        val result1 = validator.validate(part1)
        Assertions.assertTrue(result1) {
            "The validator content used internally is different from $part1"
        }
        val part2 = testContent.sliceArray(dimension.toInt() until 2*dimension.toInt())
        val result2 = validator.validate(part2)
        Assertions.assertTrue(result2) {
            "The validator content used internally is different from $part2"
        }
    }

    @Test
    fun `call validate more than one time on newly generated content`(){
        val validator = Validator(filename)
        val testRandomSource = java.util.Random(filename.hashCode().toLong())/*Random(filename.hashCode())*/
        //repeat the test 5 times
        for(i in 0 until 5){
            val testContent = ByteArray(dimension.toInt())
//            val testContent = testRandomSource.nextBytes(dimension.toInt())
            testRandomSource.nextBytes(testContent)
            val result = validator.validate(testContent)
            Assertions.assertTrue(result) {
                "The validator content used internally is different from $testContent"
            }
        }
    }

}