package com.example.kafkastudy.api

import com.example.kafkastudy.constant.Constant.MY_TOPIC
import com.example.kafkastudy.manager.KafkaProduceManager
import com.example.kafkastudy.model.MyMessage
import mu.KotlinLogging
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
class ProducerController(
    private val kafkaProduceManager: KafkaProduceManager,
) {
    private val logger = KotlinLogging.logger {}
    @GetMapping("/publish")
    fun publish(
        @RequestParam message: String,
    ): String {
        kafkaProduceManager.send(MY_TOPIC, message)
        return "Published message: $message"
    }

    @GetMapping("/publish/callback")
    fun publishWithCallback(
        @RequestParam message: String,
    ): String {
        kafkaProduceManager.send(MY_TOPIC, message) {
            logger.info { ">>> Callback executed <<<" }
        }
        return "Published message: $message"
    }

    @GetMapping("/publish/my-message")
    fun publishWithCallback(
        @ModelAttribute message: MyMessage,
    ): String {
        kafkaProduceManager.send(MY_TOPIC, message)
        return "Published message: $message"
    }
}