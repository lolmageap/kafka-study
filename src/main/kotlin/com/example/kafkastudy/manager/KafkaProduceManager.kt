package com.example.kafkastudy.manager

import com.example.kafkastudy.model.MyMessage
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component

@Component
class KafkaProduceManager(
    @Qualifier("rawKafkaTemplate") private val rawKafkaTemplate: KafkaTemplate<String, String>,
    @Qualifier("jsonKafkaTemplate") private val jsonKafkaTemplate: KafkaTemplate<String, MyMessage>,
) {
    private val logger = KotlinLogging.logger {}

    fun send(
        topic: String,
        message: String,
    ) {
        rawKafkaTemplate.send(topic, message)
        logger.info { ">>> Sending message: $message <<<" }
    }

    fun send(
        topic: String,
        message: String,
        block: () -> Unit,
    ) {
        val future = rawKafkaTemplate.send(topic, message)
        future.whenComplete { sendResult: SendResult<String, String>, exception: Throwable? ->
            if (exception != null) {
                logger.error { ">>> Failed to send message: $message due to ${exception.localizedMessage} <<<" }
            } else {
                logger.info { ">>> Sending message: $message, offset: ${sendResult.recordMetadata.offset()} <<<" }
                block()
            }
        }
    }

    fun send(
        topic: String,
        message: MyMessage,
    ) {
        jsonKafkaTemplate.send(topic, message)
        logger.info { ">>> Sending message: $message <<<" }
    }

    fun send(
        topic: String,
        message: MyMessage,
        block: () -> Unit,
    ) {
        val future = jsonKafkaTemplate.send(topic, message)
        TODO("Not yet implemented")
    }
}