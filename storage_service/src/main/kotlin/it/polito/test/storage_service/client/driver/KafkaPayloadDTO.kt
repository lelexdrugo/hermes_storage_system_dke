package it.polito.test.storage_service.client.driver


data class KafkaPayloadDTO (
    val dataToStoreDTO: DataToStoreDTO,
    val message :String
)