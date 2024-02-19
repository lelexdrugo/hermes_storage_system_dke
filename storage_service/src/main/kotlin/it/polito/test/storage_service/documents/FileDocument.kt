package it.polito.test.storage_service.documents

import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.index.CompoundIndex
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant
import java.util.*


@Document("#{@environment.getProperty('collection.suffix')}.files")
//Explicit index creation because I don't use anymore GridFsTemplate for the first file insert
//Unique index on _id is created by default
@CompoundIndex(
        name = "filename_1_uploadDate_1",
        def = "{'filename': 1, 'uploadDate': 1}",
    )
data class FileDocument (
    @Id
    var _id: ObjectId = ObjectId.get(),
    var length: Long = 0,
    var chunkSize: Long = 0,
    var uploadDate: Date = Date.from(Instant.now()),
    var md5: Long = 0,
    @Indexed(unique = true)
    var filename: String = "",
    var contentType: String = "",
    var aliases: Array<String> = arrayOf(),
    var metadata: org.bson.Document = org.bson.Document(),
    //End of GridFS fields
    var lastOffset : Long = -1
){
    init {
        //If a file is created with mongofiles, contentType is not set
        if(!metadata.containsKey("_contentType"))
            metadata["_contentType"] = "cli/mongofiles"
    }
}