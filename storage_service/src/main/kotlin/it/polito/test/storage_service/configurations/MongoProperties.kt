package it.polito.test.storage_service.configurations

import org.springframework.boot.context.properties.ConfigurationProperties


@ConfigurationProperties(prefix = "spring.data.mongodb") // to access properties in application.yml file
data class MongoProperties(
        var username: String = "",
        var password: String = "",
        var host: String = "",
        var port: String = "",
        var database: String = "",
        var authenticationDatabase: String = "",
        var uri: String = "",
        var gridFs: GridFsProperty = GridFsProperty(),
        var writeConcern: WriteConcernProperty = WriteConcernProperty(),

) {

        data class GridFsProperty(
                var bucket: String = "rcs",
                var chunkSize: Int = 261120
        )

        data class WriteConcernProperty(
                //there is a bug with the builder of the uri mongo client configuration var writeConcern: String = "MAJORITY",
                var w: String = "MAJORITY",
                var wtimeoutMillis: Long = 1000,
                var journal: Boolean = true
        )
}
