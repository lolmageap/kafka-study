package com.example.kafkastudy.example

import com.example.kafkastudy.constant.Constant.BOOTSTRAP_SERVERS
import com.example.kafkastudy.constant.Constant.EARLIEST
import com.example.kafkastudy.constant.Constant.GROUP_ID
import com.example.kafkastudy.constant.Constant.MY_TOPIC
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

object ConsumerExample {
    @JvmStatic
    fun main(args: Array<String>) {
        val config = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST)
        }

        val kafkaConsumer = KafkaConsumer<String, String>(config)
        kafkaConsumer.subscribe(listOf(MY_TOPIC))

        while (true) {
            val records = kafkaConsumer.poll(1.toSecond())
            records.forEach {
                println(">>> Received record with $it <<<")
            }
        }
    }
}

private fun Int.toSecond() = Duration.ofSeconds(this.toLong())