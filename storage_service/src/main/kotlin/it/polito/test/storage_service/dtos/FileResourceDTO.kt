package it.polito.test.storage_service.dtos

data class FileResourceDTO(
    val file: FileDTO,
    val data: ByteArray
)

data class FileResourceBlockDTO(
    val file: FileDTO,
    val data: ByteArray,
    val offset: Int,
    val requestedBlockSize: Int,
    val returnedBlockSize: Int
)