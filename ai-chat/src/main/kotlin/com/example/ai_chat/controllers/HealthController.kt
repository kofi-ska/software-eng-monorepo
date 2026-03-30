package com.example.ai_chat.controllers

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

@RestController
@RequestMapping("/api/v1/health")
class HealthController {

    @GetMapping
    suspend fun health(): Map<String,Any> {
        return mapOf(
            "status" to "UP",
            "service" to "ai-chat",
            "timestamp" to Instant.now().toString(),
        )
    }
}