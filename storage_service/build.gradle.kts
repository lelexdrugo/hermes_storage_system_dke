import org.jetbrains.kotlin.backend.common.push
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jetbrains.kotlin.ir.backend.js.compile

plugins {
	id("org.springframework.boot") version "2.7.5"
	id("io.spring.dependency-management") version "1.0.15.RELEASE"
	kotlin("jvm") version "1.6.21"
	kotlin("plugin.spring") version "1.6.21"
	kotlin("plugin.allopen") version "1.4.31"
	//id ("org.javamodularity.moduleplugin") version "1.5.0"
}

/*plugins.withType<JavaPlugin>().configureEach {
	java {
		modularity.inferModulePath.set(true)
	}
}*/

group = "it.test"
version = "5"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
	mavenCentral()
}

dependencies {
	// dependencies for logging system
	implementation(group = "ch.qos.logback", name = "logback-classic", version = "1.2.6")
	implementation("io.github.microutils:kotlin-logging-jvm:2.0.11")

	implementation("org.springframework.boot:spring-boot-starter-aop")

	//openAPI support
	implementation("org.springdoc:springdoc-openapi-data-rest:1.6.0")
	implementation("org.springdoc:springdoc-openapi-ui:1.6.0")
	implementation("org.springdoc:springdoc-openapi-kotlin:1.6.0")

	annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
	// dependency for hexString useful for ObjectId
	implementation (group ="commons-codec", name= "commons-codec", version= "1.5")
	implementation("org.springframework.boot:spring-boot-starter-data-mongodb")

	//https://stackoverflow.com/questions/60049290/closenowexception-this-stream-is-not-writeable
	implementation("org.springframework.boot:spring-boot-starter-web"){
		exclude(group = "org.springframework.boot", module = "spring-boot-starter-tomcat")
	}
	implementation("org.springframework.boot:spring-boot-starter-jetty")

	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	//THis is needed for @RequestParam validation
	implementation("org.hibernate:hibernate-validator:8.0.0.Final")

	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

	implementation("org.springframework.kafka:spring-kafka")

	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	//Add to perform date handling building the message for kafka
	//implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
	//But you need also to do objectMapper.findAndRegisterModules() on serializer and deserializer


	implementation("de.codecentric:chaos-monkey-spring-boot:2.4.5")
	//Chaos Monkey for Spring Boot offers you some built in endpoints exposed via JMX or HTTP. This allows you to change configuration at runtime.
	//Spring Boot Actuator needs to be included in your project dependencies if you want to use this feature.
	implementation ("org.springframework.boot:spring-boot-starter-actuator")

	//implementation("commons-fileupload:commons-fileupload:1.5")





	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.testcontainers:junit-jupiter:1.17.6")
	testImplementation ("org.testcontainers:mongodb:1.17.3")
	/*https://github.com/silaev/mongodb-replica-set/
	testImplementation("com.github.silaev:mongodb-replica-set:0.4.3")*/
	testImplementation("org.apache.httpcomponents:httpclient:4.5.14")


	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.testcontainers:kafka:1.17.6")

	testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.6.4")
}

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs = listOf("-Xjsr305=strict")
		jvmTarget = "17"
	}
}

//tasks.withType<JavaExec> {
//	jvmArgs = listOf("-Xms512M", "-Xmx12288M") //WTF this doesn't work in gradle
//	jvmArgs = listOf("-XX:MaxHeapSize=8192m", "-XX:InitialHeapSize=512")
//}

//This is fundamental to override the default option for JVM launched by gradle running Junit test.
//It sets: -Xms512M -Xmx4096M (minimum and max heap size)
tasks.withType<Test> {
	minHeapSize = "512M"
	maxHeapSize = "12288M"//"4096M"
}


tasks.withType<Test> {
	useJUnitPlatform()
}
/*
tasks.withType<Test>().configureEach {
	useJUnitPlatform()
	// The following jvm args fix a runtime problem where some pre-java 9 modules are not being added to module path
	doFirst {
		jvmArgs = ArrayList<String>().also{
			it.push("--module-path"); it.push(classpath.asPath);
			it.push("--add-modules"); it.push("ALL-MODULE-PATH"); // to resolve all modules in the module path to be accessed by gradle test runner
			it.push("--add-exports"); it.push("java.base/jdk.internal.misc=ALL-UNNAMED");
			it.push("--add-exports"); it.push("org.junit.platform.commons/org.junit.platform.commons.logging=ALL-UNNAMED");
			it.push("--add-exports"); it.push("org.junit.platform.commons/org.junit.platform.commons.util=ALL-UNNAMED");
			it.push("--add-reads"); it.push("it.test=ALL-UNNAMED");
			//it.push("--patch-module"); /*it.push("it.test=ALL-UNNAMED=" + files(sourceSets.test.java.outputDir).asPath);*/
		}

		classpath = files()
	}
}

tasks.withType<JavaCompile> {
	doFirst {
		options.compilerArgs = ArrayList<String>().also { it.push("--module-path"); it.push(classpath.asPath) }
		classpath = files()
	}
}

