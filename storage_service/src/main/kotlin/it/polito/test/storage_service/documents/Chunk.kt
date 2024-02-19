package it.polito.test.storage_service.documents


import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.index.CompoundIndex
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field

@Document("#{@environment.getProperty('collection.suffix')}.chunks")
//Explicit index creation because I don't use anymore GridFsTemplate for the first file insert
//Unique index on _id is created by default
@CompoundIndex(
        name = "files_id_1_n_1",
        def = "{'files_id': 1, 'n': 1}",
        unique = true
    )
data class Chunk (
    @Id
    var _id: ObjectId = ObjectId.get(),
    @Field("files_id")
    var fileId: ObjectId,
    var n: Int,
    var data: ByteArray,
)