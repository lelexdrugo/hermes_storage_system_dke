package it.polito.test.storage_service.client.unit

//import jdk.random.Xoroshiro128PlusPlus

import it.polito.test.storage_service.client.generator.Generator
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.util.random.RandomGenerator
import java.util.random.RandomGenerator.LeapableGenerator
import java.util.random.RandomGeneratorFactory

@SpringBootTest
class JavaPRNG {
    val filename: String = "helloworld"

    @Test
    fun `prove strange behaviour`(){
        val firstClassicRandom = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())
        val secondClassicRandom = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())
        val thirdClassicRandom = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())
        val fourthClassicRandom = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())
        val tenBytes = ByteArray(10)
        val oneByte = ByteArray(1)
        var accumulatorArray = ByteArray(20)
        val fiveBytes = ByteArray(5)
        val eightBytes = ByteArray(8)
        println("-------------------------------------|-------------------------------------|")
        println("Genero 1 byte 20 volte")
        for(i in 1..20) {
            firstClassicRandom.nextBytes(oneByte)
            accumulatorArray[i-1] = oneByte[0]
        }
        println(accumulatorArray.contentToString())
        accumulatorArray = ByteArray(20)
        println("-------------------------------------|-------------------------------------|")
        println("Genero 5 byte 4 volte")
        thirdClassicRandom.nextBytes(fiveBytes)
        fiveBytes.copyInto(accumulatorArray, 0, 0, 5)
        thirdClassicRandom.nextBytes(fiveBytes)
        fiveBytes.copyInto(accumulatorArray, 5, 0, 5)
        thirdClassicRandom.nextBytes(fiveBytes)
        fiveBytes.copyInto(accumulatorArray, 10, 0, 5)
        thirdClassicRandom.nextBytes(fiveBytes)
        fiveBytes.copyInto(accumulatorArray, 15, 0, 5)
        println(accumulatorArray.contentToString())
        accumulatorArray = ByteArray(20)
        println("-------------------------------------|-------------------------------------|")
        println("Genero 8 byte 2 volte")
        fourthClassicRandom.nextBytes(eightBytes)
        eightBytes.copyInto(accumulatorArray, 0, 0, 8)
        fourthClassicRandom.nextBytes(eightBytes)
        eightBytes.copyInto(accumulatorArray, 8, 0, 8)
        println(accumulatorArray.contentToString())
        println("-------------------------------------|-------------------------------------|")
        println("Genero 10 byte 2 volte")
        secondClassicRandom.nextBytes(tenBytes)
        tenBytes.copyInto(accumulatorArray, 0, 0, 10)
        secondClassicRandom.nextBytes(tenBytes)
        tenBytes.copyInto(accumulatorArray, 10, 0, 10)
        println(accumulatorArray.contentToString())
        accumulatorArray = ByteArray(20)
        println("-------------------------------------|-------------------------------------|")


        val myGen = Generator(filename)
        for (i in 1..20) {
            val res = myGen.generate(1)
            accumulatorArray[i-1] = res[0]
        }
        println("Genero 1 byte 20 volte con generator")
        println(accumulatorArray.contentToString())
        accumulatorArray = ByteArray(20)
        println("-------------------------------------|-------------------------------------|")
        val myGen2 = Generator(filename)
        println("Genero 20 Byte 1 volta con generator")
        val res = myGen2.generate(20)
        println(res.contentToString())

    }

    @Test
    fun `test new PRNG`(){
        //Xoroshiro128PlusPlus has slightly better performance than Xoshiro256PlusPlus
        val firstGenerator = RandomGeneratorFactory.of<RandomGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())
        println(firstGenerator.nextLong())
        println(firstGenerator.nextLong())
        println(firstGenerator.nextLong())

        RandomGeneratorFactory.all()
            .sorted(Comparator.comparing { obj: RandomGeneratorFactory<*> -> obj.name() })
            .forEach { factory: RandomGeneratorFactory<RandomGenerator?> ->
                println(
                    String.format(
                        "%s\t%s\t%s\t%s\t%s",
                        factory.group(),
                        factory.name(),
                        factory.isJumpable,
                        factory.isSplittable,
                        factory.isArbitrarilyJumpable
                    )
                )
            }
        println("____________________________________________________")
        val myRgJumpable = RandomGeneratorFactory.all()
            .sorted(Comparator.comparing { obj: RandomGeneratorFactory<*> -> obj.name() })
            .filter { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.isJumpable }
            .findFirst()
            .map { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.create(filename.hashCode().toLong()) }
            .orElseThrow { RuntimeException("Error creating a generator") } !!
        for (i in 1 .. 3)
            println(myRgJumpable.nextLong())

    }

    @Test
    fun `utilities functions`(){
        //First way to obtain a factory of a specific type of generator
        //Then we can create a generator of that type with seed
        val firstGenerator = RandomGeneratorFactory
            .of<RandomGenerator>("Xoroshiro128PlusPlus")
            .create(filename.hashCode().toLong())
        //Show all the factory availables
        RandomGeneratorFactory.all()
            .sorted(Comparator.comparing { obj: RandomGeneratorFactory<*> -> obj.name() })
            .forEach { factory: RandomGeneratorFactory<RandomGenerator?> ->
                println(
                    String.format(
                        "%s\t%s\t%s\t%s\t%s",
                        factory.group(),
                        factory.name(),
                        factory.isJumpable,
                        factory.isSplittable,
                        factory.isArbitrarilyJumpable
                    )
                )
            }

        val secondGenerator = RandomGeneratorFactory.all()
            //.sorted(Comparator.comparing { obj: RandomGeneratorFactory<*> -> obj.name() })
            .filter { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.isJumpable }
            .filter { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.name() == "Xoroshiro128PlusPlus" }
            .map { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.create(filename.hashCode().toLong()) }
            //.findFirst()
            //.orElseThrow { RuntimeException("Error creating a generator") } !!
    }
    @Test
    fun `impossible cast`(){
//        RandomGeneratorFactory.of<RandomGenerator.ArbitrarilyJumpableGenerator>("L32X64MixRandom").create(filename.hashCode().toLong())

        RandomGeneratorFactory.all()
            .sorted(Comparator.comparing { obj: RandomGeneratorFactory<*> -> obj.name() })
            .forEach { factory: RandomGeneratorFactory<RandomGenerator?> ->
                println(
                    String.format(
                        "%s\t%s\t%s\t%s\t%s",
                        factory.group(),
                        factory.name(),
                        factory.isJumpable,
                        factory.isSplittable,
                        factory.isArbitrarilyJumpable
                    )
                )
            }
        println("____________________________________________________")
        val myRgJumpable = RandomGeneratorFactory.all()
            .filter { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.isJumpable }
            .findAny()
            .map { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.create(3L) }
            .orElseThrow { RuntimeException("Error creating a generator") } !!
        for (i in 1 .. 3)
            println(myRgJumpable.nextLong())

/*        val myRgJumpable2: RandomGenerator.LeapableGenerator = RandomGeneratorFactory.all()
            .filter { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.isJumpable }
            .findAny()
            .map { obj: RandomGeneratorFactory<RandomGenerator?> -> obj.create(3L) }
            .orElseThrow { RuntimeException("Error creating a generator") }*/
        val v: Double = 2.0
/*        myRgJumpable2.jump()
        println(myRgJumpable2.nextLong())*/
       // val testRandomSource = RandomGeneratorFactory.of<Xoroshiro128PlusPlus>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())
       val testRandomSource2 = RandomGeneratorFactory.of<LeapableGenerator>("Xoroshiro128PlusPlus").create(filename.hashCode().toLong())

    }
}