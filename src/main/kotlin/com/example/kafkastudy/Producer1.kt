package com.example.kafkastudy

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

object Producer1 {
    private const val BOOTSTRAP_SERVERS = "localhost:9092"
    private const val TOPIC = "my-topic"
    private const val FIRST_MESSAGE = "Hello, Kafka!"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Properties().apply {
            put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ACKS_CONFIG, "all")
            put(RETRIES_CONFIG, "100")
        }

        val kafkaProducer = KafkaProducer<String, String>(config)
        val record = ProducerRecord<String, String>(TOPIC, FIRST_MESSAGE)

        val metadata = kafkaProducer.send(record).get()
        println(">>> Record sent to partition ${metadata.partition()} with offset ${metadata.offset()} <<<")

        kafkaProducer.flush()
        kafkaProducer.close()
    }
}