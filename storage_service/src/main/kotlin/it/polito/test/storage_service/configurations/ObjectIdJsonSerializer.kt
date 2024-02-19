package it.polito.test.storage_service.configurations

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.*
import org.bson.types.ObjectId
import java.io.IOException

class ObjectIdJsonSerializer : JsonSerializer<ObjectId?>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(o: ObjectId?, j: JsonGenerator, s: SerializerProvider?) {
        if (o == null) {
            j.writeNull()
        } else {
            j.writeString(o.toString())
        }
    }

/*    //Deserialization is done correctly if serialization is done with ObjectIdJsonSerializer
    To perform serialization in a class on a objectId field
    @JsonSerialize(using= ObjectIdJsonSerializer::class)*/
}