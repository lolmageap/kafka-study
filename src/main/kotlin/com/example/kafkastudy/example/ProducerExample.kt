package com.example.kafkastudy.example

import com.example.kafkastudy.constant.Constant.BOOTSTRAP_SERVERS
import com.example.kafkastudy.constant.Constant.MY_TOPIC
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

object ProducerExample {
    @JvmStatic
    fun main(args: Array<String>) {
        val config = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, "100")
        }

        val kafkaProducer = KafkaProducer<String, String>(config)
        val record = ProducerRecord<String, String>(MY_TOPIC, "Hello, Kafka!")

        val metadata = kafkaProducer.send(record).get()
        println(">>> Record sent to partition ${metadata.partition()} with offset ${metadata.offset()} <<<")

        kafkaProducer.flush()
        kafkaProducer.close()
    }
}