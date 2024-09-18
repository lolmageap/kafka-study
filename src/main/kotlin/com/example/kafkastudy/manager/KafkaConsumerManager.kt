package com.example.kafkastudy.manager

import com.example.kafkastudy.constant.Constant.GROUP_ID
import com.example.kafkastudy.constant.Constant.MY_TOPIC
import com.example.kafkastudy.model.MyMessage
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class KafkaConsumerManager(
    private val objectMapper: ObjectMapper,
) {
    private val logger = KotlinLogging.logger {}

    @KafkaListener(
        topics = [MY_TOPIC],
        groupId = GROUP_ID,
    )
    fun listenMessage(
        message: String,
    ) {
        try {
            val myMessage = objectMapper.readValue(message, MyMessage::class.java)
            logger.info { ">>> Received message: $myMessage <<<" }
        } catch (e: Exception) {
            logger.error { ">>> Failed to parse message: $message due to ${e.localizedMessage} <<<" }
        }
    }
}