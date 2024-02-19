package it.polito.test.storage_service.dtos

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import it.polito.test.storage_service.configurations.ObjectIdJsonSerializer
import it.polito.test.storage_service.documents.FileDocument
import org.bson.types.ObjectId
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty

data class FileDTO(
    @field:NotEmpty(message = "file_id can't be empty or null")
    val _id: ObjectId,
    @field:NotBlank(message = "Filename can't be empty or null")
    var filename: String,
    var length: Long,
    var chunkSize: Long,
    // Deprecated, GridFS store it in metadadata, _contentType
    var contentType: String,
    var metadata: org.bson.Document,
    var lastOffset: Long

)
fun FileDocument.toFileDTO() : FileDTO {
    return FileDTO(_id, filename, length, chunkSize, metadata.getString("_contentType"), metadata, lastOffset)
}
//Deprecated: refactor removing this and using DataToStoreDTO, used also from kafka
data class FileToCreateDTO(
    //How to send bytearray in a http request?
    val fileContent: String = "",
    val contentType: String = "data/bin"
)

data class DataToAddDTO(
    //Deserialization is done correctly if serialization is done with ObjectIdJsonSerializer
    @JsonSerialize(using= ObjectIdJsonSerializer::class)
    val fileId: ObjectId,

    @field:NotBlank(message = "Data can't be empty or null")
    val data: String,
)

data class DataToStoreDTO(
    @field:NotBlank(message = "Filename can't be empty or null")
    val filename: String,
    val fileContent: ByteArray = byteArrayOf(),
    val metadata: MutableMap<String, String> = mutableMapOf(),
)