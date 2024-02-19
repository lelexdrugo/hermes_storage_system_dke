package it.polito.test.storage_service.client.validator

import java.util.random.RandomGenerator
import java.util.random.RandomGeneratorFactory
import kotlin.random.Random

class Validator(val filename: String) {
    /**
     * Two generators with the same seed produce the same sequence of values within the same version of Kotlin runtime.
     * Note: Future versions of Kotlin may change the algorithm of this seeded number generator so that it will return a sequence of values different from the current one for a given seed.
     * On JVM the returned generator is NOT thread-safe. Do not invoke it from multiple threads without proper synchronization.
     */
    private var psSeededGenerator = Random(filename.hashCode())
    private var newGenerator = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())//java.util.Random(filename.hashCode().toLong())
    private var validatedByte = 0

    fun validate(content: ByteArray): Boolean {
        val dimension = content.size
        //val generatedValue = psSeededGenerator.nextBytes(dimension)
        val generatedValue = ByteArray(dimension)
        //newGenerator.nextBytes(generatedValue)
        for (i in 0 until dimension) {
            generatedValue[i] = newGenerator.nextLong().toByte()
        }
        //In this way a keep state of the produced byte
        validatedByte+=dimension
        //for debugging
        if(!generatedValue.contentEquals(content)) {
            println("generatedValue: size ${generatedValue.size};\n" +
                    "first 30 bytes: ${generatedValue.sliceArray(0..29).contentToString()};\n" +
            "contentToValidate: size ${dimension};\n" +
                    "first 30 bytes: ${content.sliceArray(0..29).contentToString()}\n" +
            "generated - last 30 bytes: ${generatedValue.sliceArray(generatedValue.size-30..generatedValue.size-1).contentToString()}\n" +
            "contentToValidate - last 30 bytes: ${content.sliceArray(content.size-30..content.size-1).contentToString()}")

        }
        return generatedValue.contentEquals(content)
    }
    //I have to reset the generator if I seek backward in the file I'm validating or skip some bytes if I seek forward
    fun seekToPosition(index: Int){
        if(index< validatedByte){
            //Reset the generator
            newGenerator = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())//java.util.Random(filename.hashCode().toLong())
            validatedByte = index/*+1*/
            //move the "index" of the internal generator to the right position
            val toSkip = ByteArray(index)
            for (i in 0 until index) {
                toSkip[i] = newGenerator.nextLong().toByte()
            }
            //newGenerator.nextBytes(toSkip)
            println("toSKip = $toSkip")
        }
        else if(index> validatedByte){
            //move the "index" of the internal generator to the right position
            val toSkip = ByteArray(index-validatedByte)
            for (i in 0 until (index-validatedByte)) {
                toSkip[i] = newGenerator.nextLong().toByte()
            }
            validatedByte=index
            //newGenerator.nextBytes(toSkip)
            println("toSKip = ${toSkip.contentToString()}")
        }
    }
/*    fun seekToPosition(index: Int){
        if(index< validatedByte){
            //Reset the generator
            psSeededGenerator = Random(filename.hashCode())
            //move the "index" to the right position
            psSeededGenerator.nextBytes(index)
        }
        else if(index> validatedByte){
            //move the "index" of the internal generator to the right position
            psSeededGenerator.nextBytes(index-validatedByte)
        }
    }*/
}