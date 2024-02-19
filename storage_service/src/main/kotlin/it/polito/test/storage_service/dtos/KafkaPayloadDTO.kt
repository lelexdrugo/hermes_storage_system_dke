package it.polito.test.storage_service.dtos


data class KafkaPayloadDTO (
    val dataToStoreDTO: DataToStoreDTO,
    val message :String
)