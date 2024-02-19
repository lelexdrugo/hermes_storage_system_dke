package it.polito.test.storage_service.client.integration

import it.polito.test.storage_service.client.generator.Generator
import it.polito.test.storage_service.client.validator.Validator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import kotlin.random.Random

/**
* Aim of this test suite is to check compatibility between class Generator and Validator, without using Driver and so Kafka.
 * This is important because I will use these two class for further test like the one with two client or the endToEndTest inside StorageService
 */
@SpringBootTest
class GenerationAndValidation {
    val filename: String = Random.nextLong().toString()
    val dimension: Long = 100L

    @Test
    //call generate one time and then validate
    fun `call generate one time and then validate`(){
        val generator = Generator(filename)
        val validator = Validator(filename)
        val generatedContent = generator.generate(dimension)
        val validationResult = validator.validate(generatedContent)
        Assertions.assertTrue(validationResult) {
            "The validation of the generated content is $validationResult and not true. Generator and validator are not aligned"
        }
    }

    @Test
    fun `call generate with a generator and then validate with a validator with a different filename`(){
        val generator = Generator(filename)
        val validator = Validator(filename+"fake")
        val generatedContent = generator.generate(dimension)
        val validationResult = validator.validate(generatedContent)
        Assertions.assertFalse(validationResult) {
            "The validation of the generated content is $validationResult and not false. Generator and validator are aligned even with different seed"
        }
    }

    @Test
    fun `call generate more than one time and then validate`(){
        val generator = Generator(filename)
        val validator = Validator(filename)
        val generatedContent = generator.generate(dimension)
        val validationResult = validator.validate(generatedContent)
        Assertions.assertTrue(validationResult) {
            "The validation of the generated content is $validationResult and not true"
        }
        val generatedContent2 = generator.generate(dimension)
        val validationResult2 = validator.validate(generatedContent2)
        Assertions.assertTrue(validationResult2) {
            "The validation of the generated content is $validationResult2 and not true. Generator and validator are not aligned"
        }
    }

    @Test
    fun `call generate more than one time and then validate second piece before the first`(){
        val generator = Generator(filename)
        val validator = Validator(filename)
        val generatedContent = generator.generate(dimension)
        val generatedContent2 = generator.generate(dimension)
        val validationResult = validator.validate(generatedContent2)
        Assertions.assertFalse(validationResult) {
            "The validation of the generated content is $validationResult and not false. Generator and validator are aligned even if the second piece is validated before the first"
        }
    }

    @Test
    fun `call generate a lot of time and then validate`(){
        val generator = Generator(filename)
        val validator = Validator(filename)
        for (i in 1..1000) {
            val generatedContent = generator.generate(dimension)
            val validationResult = validator.validate(generatedContent)
            Assertions.assertTrue(validationResult) {
                "The validation of the generated content is $validationResult and not true. Generator and validator are not aligned"
            }
        }
    }

    @Test
    fun `call generate a lot of time with different dimension and then validate`(){
        val generator = Generator(filename)
        val validator = Validator(filename)
        for (i in 1..1000) {
            val generatedContent = generator.generate(Random.nextInt(1, 100000).toLong())
            val validationResult = validator.validate(generatedContent)
            Assertions.assertTrue(validationResult) {
                "The validation of the generated content is $validationResult and not true. Generator and validator doesn't work with changing dimension"
            }
        }
    }

    @Test
    fun `call generate a lot of time with maximum dimension and then validate`(){
        val generator = Generator(filename)
        val validator = Validator(filename)
        for (i in 1..1000) {
            val generatedContent = generator.generate((Int.MAX_VALUE).toLong())
            val validationResult = validator.validate(generatedContent)
            Assertions.assertTrue(validationResult) {
                "The validation of the generated content is $validationResult and not true. Generator doesn't work with max Int dimension"
            }
        }
    }


    @Test
    fun `call generate for a big content and then validate piece by piece`(){
        val generator = Generator(filename)
        val validator = Validator(filename)
        val numberOfPiece = 100
        val contentDimension = numberOfPiece*dimension
        val generatedContent = generator.generate(contentDimension)
        for (i in 0 until numberOfPiece) {
            val validationResult = validator.validate(generatedContent.sliceArray(i*dimension.toInt() until (i*dimension+dimension).toInt()))
            Assertions.assertTrue(validationResult) {
                "The validation of the generated content is $validationResult and not true. Generator doesn't work with max Int dimension"
            }
        }
    }

}