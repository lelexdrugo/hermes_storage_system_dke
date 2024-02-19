package it.polito.test.storage_service.client.unit.validator

import it.polito.test.storage_service.client.validator.Validator
import org.springframework.boot.test.context.SpringBootTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.random.RandomGenerator
import java.util.random.RandomGeneratorFactory
import kotlin.random.Random

@SpringBootTest
class SeekToPosition {
    val filename: String = Random.nextLong().toString()
    val dimension: Long = 100L

    fun randomSourceToUse(seed: String): RandomGenerator {
        return RandomGeneratorFactory
            .of<RandomGenerator>("Xoroshiro128PlusPlus")
            .create(seed.hashCode().toLong())
    }

    @Test
    fun `SIMPLY instantiate a validator, generate content and seek forward to a position`(){
        val validator = Validator(filename)
        val testRandomSource =randomSourceToUse(filename)
        val testContent = ByteArray(3)
        for (i in 0..2) {
            testContent[i] = testRandomSource.nextLong().toByte()
        }
        val firstPart = testContent.take(1).toByteArray()
        val result = validator.validate(firstPart)
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        val seekedContent = testContent.drop(2).toByteArray()

        validator.seekToPosition(2)
        val resultAfterSeek = validator.validate(seekedContent)
        Assertions.assertTrue(resultAfterSeek) {
            "The validator content used internally is different from $seekedContent"
        }
    }

    @Test
    fun `instantiate a validator, generate content and seek forward to a position`(){
        val validator = Validator(filename)
        val newRandomSource = randomSourceToUse(filename)
        val contentLength = 10*dimension
        val firstValidationLength = dimension
        val seekPosition = 5*dimension

        val testContent = ByteArray(contentLength.toInt())
        for (i in 0 until contentLength.toInt()) {
            testContent[i] = newRandomSource.nextLong().toByte()
        }

        val result = validator.validate(testContent.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        val seekedContent = testContent.drop(seekPosition.toInt()).toByteArray()

        validator.seekToPosition(seekPosition.toInt()/*-1-4*/)
        val resultAfterSeek = validator.validate(seekedContent)
        Assertions.assertTrue(resultAfterSeek) {
            "The validator content used internally is different from $seekedContent"
        }
    }

    @Test
    fun `instantiate a validator, generate content and seek backward to a position`(){
        val validator = Validator(filename)

        val newRandomSource = randomSourceToUse(filename)
        val contentLength = 10*dimension
        val firstValidationLength = 5*dimension
        val seekPosition = dimension

        val testContent = ByteArray(contentLength.toInt())
        for (i in 0 until contentLength.toInt()) {
            testContent[i] = newRandomSource.nextLong().toByte()
        }

        val result = validator.validate(testContent.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        val seekedContent = testContent.drop(seekPosition.toInt()).toByteArray()

        validator.seekToPosition(seekPosition.toInt()/*-1-4*/)
        val resultAfterSeek = validator.validate(seekedContent)
        Assertions.assertTrue(resultAfterSeek) {
            "The validator content used internally is different from $seekedContent"
        }
    }

    @Test
    fun `instantiate a validator, generate content and seek forward to a position and then backward to a position`(){
        val validator = Validator(filename)

        val newRandomSource = randomSourceToUse(filename)
        val contentLength = 10*dimension
        val firstValidationLength = dimension
        val seekPosition = 5*dimension
        val seekBackwardPosition = 2*dimension

        val testContent = ByteArray(contentLength.toInt())
        for (i in 0 until contentLength.toInt()) {
            testContent[i] = newRandomSource.nextLong().toByte()
        }

        val result = validator.validate(testContent.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        val seekedContent = testContent.drop(seekPosition.toInt()).toByteArray()

        validator.seekToPosition(seekPosition.toInt()/*-1-4*/)
        val resultAfterSeek = validator.validate(seekedContent)
        Assertions.assertTrue(resultAfterSeek) {
            "The validator content used internally is different from $seekedContent"
        }
        val seekedBackwardContent = testContent.drop(seekBackwardPosition.toInt()).toByteArray()

        validator.seekToPosition(seekBackwardPosition.toInt()/*-1-4*/)
        val resultAfterSeek2 = validator.validate(seekedBackwardContent)
        Assertions.assertTrue(resultAfterSeek2) {
            "The validator content used internally is different from $seekedBackwardContent"
        }
    }

    @Test
    fun `instantiate a validator, generate content and seek forward multiple times`() {
        val validator = Validator(filename)

        val newRandomSource = randomSourceToUse(filename)
        val dimensionMultiplier = 10
        val contentLength = dimensionMultiplier * dimension
        val firstValidationLength = dimension
        val testContent = ByteArray(contentLength.toInt())
        for (i in 0 until contentLength.toInt()) {
            testContent[i] = newRandomSource.nextLong().toByte()
        }


        val result = validator.validate(testContent.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        for (i in 2..dimensionMultiplier) {
            val seekForwardPosition = i*dimension
            val seekedContent = testContent.drop(seekForwardPosition.toInt()).toByteArray()
            validator.seekToPosition(seekForwardPosition.toInt())
            val resultAfterSeek = validator.validate(seekedContent.take((firstValidationLength/2).toInt()).toByteArray())
            Assertions.assertTrue(resultAfterSeek) {
                "The validator content used internally is different from $seekedContent"
            }
        }
    }

    @Test
    fun `instantiate a validator, generate content and seek backward multiple times`() {
        val validator = Validator(filename)

        val newRandomSource = randomSourceToUse(filename)
        val dimensionMultiplier = 10
        val contentLength = dimensionMultiplier * dimension
        val testContent = ByteArray(contentLength.toInt())
        for (i in 0 until contentLength.toInt()) {
            testContent[i] = newRandomSource.nextLong().toByte()
        }

        val result = validator.validate(testContent)
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        var i = dimensionMultiplier
        while (--i > 0) {
            val seekBackwardPosition = i*dimension
            val seekedContent = testContent.drop(seekBackwardPosition.toInt()).toByteArray()
            validator.seekToPosition(seekBackwardPosition.toInt())
            val resultAfterSeek = validator.validate(seekedContent)
            Assertions.assertTrue(resultAfterSeek) {
                "The validator content used internally is different from $seekedContent"
            }
        }
    }

    @Test
    fun `instantiate a validator, generate content, seek forward to a position and then validate again`(){
        val validator = Validator(filename)

        val newRandomSource = randomSourceToUse(filename)
        val contentLength = 10*dimension
        val firstValidationLength = dimension
        val seekPosition = 5*dimension

        val testContent = ByteArray(contentLength.toInt())
        for (i in 0 until contentLength.toInt()) {
            testContent[i] = newRandomSource.nextLong().toByte()
        }

        val result = validator.validate(testContent.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        val seekedContent = testContent.drop(seekPosition.toInt()).toByteArray()

        validator.seekToPosition(seekPosition.toInt()/*-1-4*/)

        val resultAfterSeek = validator.validate(seekedContent.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(resultAfterSeek) {
            "The validator content used internally after seek is different from $seekedContent"
        }

        val secondValidationLength = seekPosition+firstValidationLength
        val nextContent1 = testContent.drop(secondValidationLength.toInt()).toByteArray()
        val result2 = validator.validate(nextContent1.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(result2) {
            "The validator content used internally in the first validation after seek is different from $seekedContent"
        }
        val nextContent2 = nextContent1.drop(firstValidationLength.toInt()).toByteArray()
        val result3 = validator.validate(nextContent2)
        Assertions.assertTrue(result3) {
            "The validator content used internally in the second validation after seek is different from $seekedContent"
        }
    }

    @Test
    fun `instantiate a validator, generate content, seek backward to a position and then validate again`(){
        val validator = Validator(filename)

        val newRandomSource = randomSourceToUse(filename)
        val contentLength = 10*dimension
        val firstValidationLength = 5*dimension
        val seekPosition = dimension

        val testContent = ByteArray(contentLength.toInt())
        for (i in 0 until contentLength.toInt()) {
            testContent[i] = newRandomSource.nextLong().toByte()
        }

        val result = validator.validate(testContent.take(firstValidationLength.toInt()).toByteArray())
        Assertions.assertTrue(result) {
            "The validator content used internally is different from $testContent"
        }
        val seekedContent = testContent.drop(seekPosition.toInt()).toByteArray()

        validator.seekToPosition(seekPosition.toInt())


        val validationLengthAfterBackwardSeek = dimension
        val resultAfterSeek = validator.validate(seekedContent.take(validationLengthAfterBackwardSeek.toInt()).toByteArray())
        Assertions.assertTrue(resultAfterSeek) {
            "The validator content used internally after seek is different from $seekedContent"
        }

        val secondValidationLength = seekPosition+validationLengthAfterBackwardSeek
        val nextContent1 = testContent.drop(secondValidationLength.toInt()).toByteArray()
        val result2 = validator.validate(nextContent1.take(validationLengthAfterBackwardSeek.toInt()).toByteArray())
        Assertions.assertTrue(result2) {
            "The validator content used internally in the first validation after seek is different from $seekedContent"
        }
        val nextContent2 = nextContent1.drop(validationLengthAfterBackwardSeek.toInt()).toByteArray()
        val result3 = validator.validate(nextContent2)
        Assertions.assertTrue(result3) {
            "The validator content used internally in the second validation after seek is different from $seekedContent"
        }
    }
}