package com.example.kafkastudy.config

import com.example.kafkastudy.constant.Constant.BOOTSTRAP_SERVERS
import com.example.kafkastudy.constant.Constant.EARLIEST
import com.example.kafkastudy.constant.Constant.GROUP_ID
import com.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@Configuration
class KafkaConsumerConfig {
    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> =
        DefaultKafkaConsumerFactory(
            mapOf(
                BOOTSTRAP_SERVERS_CONFIG to BOOTSTRAP_SERVERS,
                GROUP_ID_CONFIG to GROUP_ID,
                KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                AUTO_OFFSET_RESET_CONFIG to EARLIEST,
            )
        )

    @Bean
    fun kafkaListenerContainerFactory() =
        ConcurrentKafkaListenerContainerFactory<String, String>().apply {
            consumerFactory = consumerFactory()
        }
}