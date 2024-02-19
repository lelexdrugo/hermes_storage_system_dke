package it.polito.test.storage_service

import it.polito.test.storage_service.configurations.KafkaProperties
import it.polito.test.storage_service.configurations.MongoProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration
import org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.web.servlet.config.annotation.CorsRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer

@SpringBootApplication(exclude=[MongoAutoConfiguration::class,
	MongoRepositoriesAutoConfiguration::class,
	MongoDataAutoConfiguration::class])
@EnableConfigurationProperties(KafkaProperties::class, MongoProperties::class)
class StorageService{
	@Bean
	fun cors(): WebMvcConfigurer? {
		return object : WebMvcConfigurer {
			override fun addCorsMappings(registry: CorsRegistry) {
				registry.addMapping("/**")
			}
		}
	}
}

fun main(args: Array<String>) {
	runApplication<StorageService>(*args)
}
