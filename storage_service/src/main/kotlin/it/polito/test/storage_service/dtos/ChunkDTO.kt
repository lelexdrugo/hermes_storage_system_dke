package it.polito.test.storage_service.dtos

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import it.polito.test.storage_service.configurations.ObjectIdJsonSerializer
import org.bson.types.ObjectId

data class ChunkDTO(
    //Deserialization is done correctly if serialization is done with ObjectIdJsonSerializer
    @JsonSerialize(using= ObjectIdJsonSerializer::class)
    val fileId: ObjectId,

    val data: ByteArray,
)
