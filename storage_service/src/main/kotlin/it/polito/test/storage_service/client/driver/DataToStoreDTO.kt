package it.polito.test.storage_service.client.driver

data class DataToStoreDTO(
    val filename: String,
    val fileContent: ByteArray = byteArrayOf(),
    val metadata: MutableMap<String, String> = mutableMapOf(),
)