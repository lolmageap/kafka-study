package com.example.kafkastudy

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

object Consumer1 {
    private const val BOOTSTRAP_SERVERS = "localhost:9092"
    private const val TOPIC = "my-topic"
    private const val GROUP_ID = "test-group"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Properties().apply {
            put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(GROUP_ID_CONFIG, GROUP_ID)
            put(AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        val kafkaConsumer = KafkaConsumer<String, String>(config)
        kafkaConsumer.subscribe(listOf(TOPIC))

        while (true) {
            val records = kafkaConsumer.poll(1.toSecond())
            records.forEach {
                println(">>> Received record with $it <<<")
            }
        }
    }
}

private fun Int.toSecond() = Duration.ofSeconds(this.toLong())