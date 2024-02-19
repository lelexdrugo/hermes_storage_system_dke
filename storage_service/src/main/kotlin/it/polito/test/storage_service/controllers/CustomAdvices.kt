package it.polito.test.storage_service.controllers

import org.springframework.http.HttpStatus
import org.springframework.web.bind.MissingServletRequestParameterException
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.bind.annotation.ResponseStatus
import java.util.*
import javax.validation.ConstraintViolationException

@ControllerAdvice
class CustomAdvice {
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(ConstraintViolationException::class)
    fun validationFailed(e: ConstraintViolationException): ErrorDetails {
        return ErrorDetails(
            Date(), e.message!!, "Validation failed"
        )
    }

    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MissingServletRequestParameterException::class)
    fun parameterBindingFailed(e: MissingServletRequestParameterException): ErrorDetails {
        return ErrorDetails(
            Date(), e.message, "Binding failed"
        )
    }
}
data class ErrorDetails(
    val dateTime: Date,
    val message: String,
    val details: String)
