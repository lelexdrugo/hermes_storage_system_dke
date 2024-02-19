package it.polito.test.storage_service.exceptions

open class FileException (message: String) : Exception(message)

//@ResponseStatus(value = HttpStatus.NOT_FOUND)
class FileNotFoundException (message: String) : FileException(message)