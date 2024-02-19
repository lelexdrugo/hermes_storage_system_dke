package it.polito.test.storage_service.repositories

import it.polito.test.storage_service.documents.Chunk
import org.bson.types.ObjectId
import org.springframework.data.mongodb.repository.Aggregation
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.data.mongodb.repository.Query
import org.springframework.data.mongodb.repository.Update
import org.springframework.stereotype.Repository

@Repository
interface ChunkRepository : MongoRepository<Chunk, String> {
    fun findAllByFileId(fileId: ObjectId): List<Chunk>
    fun countAllByFileId(fileId: ObjectId): Long
    fun findByFileIdAndN(fileId: ObjectId, n: Int): Chunk

    fun findFirstByFileIdOrderByNDesc(fileId: ObjectId): Chunk

    fun findAllByFileIdAndNBetween(fileId: ObjectId, start: Int, end: Int): List<Chunk>
//    fun findAllByFileIdAndNGreaterThanEqualAndNLessThanEqual(fileId: ObjectId, start: Int, end: Int): List<Chunk>
    @Query("{'files_id' : ?0, 'n' : { '\$gte' : ?1, '\$lte' : ?2}}")
    fun findAllByFileIdAndNBetweenInclusive(fileId: ObjectId, start: Int, end: Int): List<Chunk>

    //Use, whenever is possible, find method with projection, to improve performance
    @Update("{ '\$set' : { 'data' : ?1 } }")
    fun findAndSetDataBy_id(_id: ObjectId, data: ByteArray): Long

/*    //My aggregation query:
    db.rcs.chunks.aggregate([
    {
        $match: {
        "files_id": ObjectId("63e38caccc9beb65dfd37478")
        }
    },
    {
        $project:{
        files_id: 1,
        "size": { $binarySize: "$data" }
        }
    },
    {
        $group: {
        _id: "$files_id",
        totalValue :{ $sum: "$size" }
        }
    }
    ])*/


    @Aggregation(pipeline = ["{ '\$match': { 'files_id': ?0 } }"])
    fun sumDataSizeByFileId(fileId: ObjectId): Int?

    @Aggregation(pipeline = ["{ '\$match': { 'files_id': ?0 } }", "{ '\$project': { 'files_id': 1, 'size': { '\$binarySize': '\$data' } } }","{ '\$group': { '_id': '\$files_id', 'totalValue' :{ '\$sum': '\$size' } }}"])
    fun sumDataSizeByFileId2(fileId: ObjectId): Int
}