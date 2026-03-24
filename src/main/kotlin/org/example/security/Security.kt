package org.example.security

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class SecurityGateway {
    private val rateLimits = ConcurrentHashMap<UUID, AtomicLong>()
    private val LIMIT = 100 // 100 requests per window

    fun authenticate(token: String): UUID? {
        // Mock JWT: just return a UUID if token is not empty
        return if (token.isNotBlank()) UUID.nameUUIDFromBytes(token.toByteArray()) else null
    }

    fun checkRateLimit(userId: UUID): Boolean {
        val count = rateLimits.getOrPut(userId) { AtomicLong(0) }
        return count.incrementAndGet() <= LIMIT
    }
    
    fun resetRateLimits() {
        rateLimits.clear()
    }
}
