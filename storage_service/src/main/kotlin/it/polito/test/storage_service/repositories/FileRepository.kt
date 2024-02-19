package it.polito.test.storage_service.repositories

import it.polito.test.storage_service.documents.FileDocument
import org.bson.types.ObjectId
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.data.mongodb.repository.Update
import org.springframework.stereotype.Repository

@Repository
interface FileRepository : MongoRepository<FileDocument, String> {
    fun findByFilename(filename: String) : FileDocument
    fun findBy_id(_id: ObjectId) : FileDocument
    fun existsByFilename(filename: String) : Boolean

    //I can also use save because MongoRepository extend CrudRepository, but I read that is not atomic
    @Update("{ '\$inc' : { 'length' : ?1 }}")
    fun findAndIncrementLengthBy_id(_id: ObjectId, increment: Long)
    @Update("{ '\$set' : { 'lastOffset' : ?1 }}")
    fun findAndSetLastOffsetBy_id(_id: ObjectId, currentOffset: Long)
    @Update("{ '\$set' : { 'lastOffset' : ?1}, '\$inc' : { 'length' : ?2 }}")
    fun findAndSetLastOffsetAndIncrementLengthBy_id(_id: ObjectId, currentOffset: Long, increment: Long): Long
}