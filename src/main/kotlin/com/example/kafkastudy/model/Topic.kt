package com.example.kafkastudy.model

@JvmInline
value class Topic(
    val value: String,
) {
    companion object {
        fun of(
            value: String,
        ) =
            Topic(value)
    }
}