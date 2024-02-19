package it.polito.client_producer.generator
import org.slf4j.LoggerFactory
import kotlin.random.Random
import java.util.Random as Random1

const val KIBIBYTE = 1024L
const val MEBIBYTE = 1024 * KIBIBYTE
const val GIBIBYTE = 1024 * MEBIBYTE
const val MIN_FILE_SIZE = 1L //* KIBIBYTE
//const val MAX_FILE_SIZE = 10 * GIBIBYTE
//const val MAX_FILE_SIZE = 10 * MEBIBYTE
//const val MAX_BYTEARRAY_SIZE_FOUND = 268435456L
const val MAX_BYTEARRAY_SIZE_FOUND = 134217728L

class Generator(filename: String){

    /**
     * Two generators with the same seed produce the same sequence of values within the same version of Kotlin runtime.
     * Note: Future versions of Kotlin may change the algorithm of this seeded number generator so that it will return a sequence of values different from the current one for a given seed.
     * On JVM the returned generator is NOT thread-safe. Do not invoke it from multiple threads without proper synchronization.
     */
    private val psSeededGenerator = Random(filename.hashCode())
    //Add a companion object to return a map of filename and their dimension
    companion object {
        //instantiate kotlin logger
        private val logger = LoggerFactory.getLogger(Generator::class.java)
        private val files: MutableMap<String, Long> = mutableMapOf()
        private const val seedForFileMapDimension = 42
        private val psFileMapGenerator = Random(seedForFileMapDimension)
/*        //Initialize for default use
        init {
            for(i in 1..3){
                files["file$i"] = psFileMapGenerator.nextLong(MIN_FILE_SIZE, 10* MEBIBYTE*//*1* GIBIBYTE*//*)
            }
        }*/
        //Call for custom map setting
        //I cannot obtain the same file map from the generator, but in this way I have randomisation on the filename and so on partition division
        fun setFileMapProperty(numberOfFiles: Int, maxDimension: Long= MAX_BYTEARRAY_SIZE_FOUND, fixedMaxDimension: Boolean = false): MutableMap<String, Long>{
            val nameGenerator = Random1()
            val maxDimensionToUse =  if(maxDimension< MIN_FILE_SIZE) MAX_BYTEARRAY_SIZE_FOUND else maxDimension
            if(maxDimension< MIN_FILE_SIZE)
                logger.info("Max dimension is too small. Set to default value: $MAX_BYTEARRAY_SIZE_FOUND")
            if(fixedMaxDimension)
                for(i in 1..numberOfFiles) {
                    files[nameGenerator.nextLong().toString()] = maxDimensionToUse
                }
            else {
                for (i in 1..numberOfFiles)
                    files[nameGenerator.nextLong().toString()] = psFileMapGenerator.nextLong(MIN_FILE_SIZE, maxDimensionToUse)
            }

            return files
        }
        fun setFileMapPropertyWithTalkingName(numberOfFiles: Int, maxDimension: Long= MAX_BYTEARRAY_SIZE_FOUND, fixedMaxDimension: Boolean = false){
            val maxDimensionToUse =  if(maxDimension< MIN_FILE_SIZE) MAX_BYTEARRAY_SIZE_FOUND else maxDimension
            if(maxDimension< MIN_FILE_SIZE)
                logger.info("Max dimension is too small. Set to default value: $MAX_BYTEARRAY_SIZE_FOUND")
            if(fixedMaxDimension)
                for(i in 1..numberOfFiles)
                    files["file$i"] = maxDimensionToUse
            else {
                for (i in 1..numberOfFiles)
                    files["file$i"] = psFileMapGenerator.nextLong(MIN_FILE_SIZE, maxDimensionToUse)
            }
        }

        fun getFiles(): MutableMap<String, Long> {
            if(files.isEmpty()){
                //Initialize for default use. No Custom settings used
                val nameGenerator = Random1()
                for(i in 1..3){
                    files[nameGenerator.nextLong().toString()] = psFileMapGenerator.nextLong(MIN_FILE_SIZE, MAX_BYTEARRAY_SIZE_FOUND)
                }
            }
            return files
        }


        fun maxDimension(): Long{
            return MAX_BYTEARRAY_SIZE_FOUND
        }
    }


    private val newGenerator = Random1(filename.hashCode().toLong())
//    private val newGenerator = RandomGenerator.ArbitrarilyJumpableGenerator.of("L32X64MixRandom")
//     This one was good but in docker it doesn't include jdk.random because I've to add java module during compilation... but it's a mess
    // https://stackoverflow.com/questions/75201154/jdeps-does-not-add-jdk-random-when-using-randomgenerator-getdefault
//    private val newGenerator  = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())


    /**
     * Int.MAX_VALUE is 2,147,483,647
     * ByteArray(dimension) throws an exception if dimension is greater than 536870911 (INT_MAX_VALUE/4)
     * Between 505570911 and 505470911
     * So a temp solution is accept a Int and trim it to 268435456
     */

    fun generateOld(dimension: Long): ByteArray{
        //TODO: fix this: what happen with a long greater than Int.MAX_VALUE?
        if(dimension > MAX_BYTEARRAY_SIZE_FOUND){
            logger.warn("Dimension is too big. Set to default value: $MAX_BYTEARRAY_SIZE_FOUND")
            val ba = ByteArray(MAX_BYTEARRAY_SIZE_FOUND.toInt())
            newGenerator.nextBytes(ba)
            return ba
        }
        val ba = ByteArray(dimension.toInt())
        newGenerator.nextBytes(ba)
        return ba//psSeededGenerator.nextBytes(dimension.toInt())
    }
    fun generate(dimension: Long): ByteArray{
        //TODO: fix this: what happen with a long greater than Int.MAX_VALUE?
        if(dimension > MAX_BYTEARRAY_SIZE_FOUND){
            logger.warn("Dimension is too big. Set to default value: $MAX_BYTEARRAY_SIZE_FOUND")
            val ba = ByteArray(MAX_BYTEARRAY_SIZE_FOUND.toInt())
            for (i in 0 until MAX_BYTEARRAY_SIZE_FOUND.toInt()) {
                ba[i] = newGenerator.nextLong().toByte()
            }
            return ba
        }
        val ba = ByteArray(dimension.toInt())

        for (i in 0 until dimension.toInt()) {
            ba[i] = newGenerator.nextLong().toByte()
        }
        return ba//psSeededGenerator.nextBytes(dimension.toInt())
    }
    fun effGenerate(dimension: Long, ba: ByteArray): ByteArray{
        //TODO: fix this: what happen with a long greater than Int.MAX_VALUE?
        if(dimension > MAX_BYTEARRAY_SIZE_FOUND){
            logger.warn("Dimension is too big. Set to default value: $MAX_BYTEARRAY_SIZE_FOUND")
            for (i in 0 until MAX_BYTEARRAY_SIZE_FOUND.toInt()) {
                ba[i] = newGenerator.nextLong().toByte()
            }
            return ba
        }
        for (i in 0 until dimension.toInt()) {
            ba[i] = newGenerator.nextLong().toByte()
        }
        return ba//psSeededGenerator.nextBytes(dimension.toInt())
    }
}
