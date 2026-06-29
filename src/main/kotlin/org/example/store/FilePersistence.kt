package org.example.store

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.example.domain.Side
import org.example.events.*
import org.slf4j.LoggerFactory
import java.io.File
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.UUID

class FilePersistence(
    private val filePath: String,
    private val scope: CoroutineScope
) {
    private val logger = LoggerFactory.getLogger(FilePersistence::class.java)
    private val persistChannel = Channel<Event>(Channel.UNLIMITED)
    private val file = File(filePath)

    init {
        if (!file.exists()) {
            file.createNewFile()
        }
        
        scope.launch(Dispatchers.IO) {
            for (event in persistChannel) {
                try {
                    val line = "${event.javaClass.simpleName}|${event.aggregateId}|${event.id}|${event.timestamp}|${event.version}|${serializePayload(event)}\n"
                    Files.write(file.toPath(), line.toByteArray(), StandardOpenOption.APPEND)
                } catch (e: Exception) {
                    logger.error("Failed to persist event", e)
                }
            }
        }
    }

    suspend fun persist(event: Event) {
        persistChannel.send(event)
    }

    fun loadEvents(): List<Event> {
        if (!file.exists()) return emptyList()
        val events = mutableListOf<Event>()
        file.forEachLine { line ->
            if (line.isNotBlank()) {
                val event = deserializeEvent(line)
                if (event != null) {
                    events.add(event)
                }
            }
        }
        return events
    }

    private fun serializePayload(event: Event): String {
        return when (event) {
            is OrderPlaced -> "orderId=${event.orderId}&userId=${event.userId}&side=${event.side}&price=${event.price}&quantity=${event.quantity}"
            is OrderMatched -> "makerOrderId=${event.makerOrderId}&takerOrderId=${event.takerOrderId}&makerUserId=${event.makerUserId}&takerUserId=${event.takerUserId}&price=${event.price}&quantity=${event.quantity}&side=${event.side}"
            is OrderCancelled -> "orderId=${event.orderId}&userId=${event.userId}"
            is BalanceChanged -> "currency=${event.currency}&amount=${event.amount}&reason=${event.reason}"
            is BalanceLocked -> "currency=${event.currency}&amount=${event.amount}"
            is BalanceUnlocked -> "currency=${event.currency}&amount=${event.amount}"
        }
    }

    private fun deserializeEvent(line: String): Event? {
        val parts = line.trim().split("|", limit = 6)
        if (parts.size < 6) return null
        val type = parts[0]
        val aggregateId = parts[1]
        val id = runCatching { UUID.fromString(parts[2]) }.getOrNull() ?: return null
        val timestamp = runCatching { Instant.parse(parts[3]) }.getOrNull() ?: return null
        val version = parts[4].toIntOrNull() ?: return null
        val params = parsePayload(parts[5])

        fun req(key: String): String? = params[key]?.takeIf { it.isNotEmpty() }

        return runCatching {
            when (type) {
                "OrderPlaced" -> OrderPlaced(
                    id = id,
                    aggregateId = aggregateId,
                    timestamp = timestamp,
                    version = version,
                    orderId = UUID.fromString(req("orderId") ?: return null),
                    userId = UUID.fromString(req("userId") ?: return null),
                    side = Side.valueOf(req("side") ?: return null),
                    price = BigDecimal(req("price") ?: return null),
                    quantity = BigDecimal(req("quantity") ?: return null)
                )
                "OrderMatched" -> OrderMatched(
                    id = id,
                    aggregateId = aggregateId,
                    timestamp = timestamp,
                    version = version,
                    makerOrderId = UUID.fromString(req("makerOrderId") ?: return null),
                    takerOrderId = UUID.fromString(req("takerOrderId") ?: return null),
                    makerUserId = UUID.fromString(req("makerUserId") ?: return null),
                    takerUserId = UUID.fromString(req("takerUserId") ?: return null),
                    price = BigDecimal(req("price") ?: return null),
                    quantity = BigDecimal(req("quantity") ?: return null),
                    side = Side.valueOf(req("side") ?: return null)
                )
                "OrderCancelled" -> OrderCancelled(
                    id = id,
                    aggregateId = aggregateId,
                    timestamp = timestamp,
                    version = version,
                    orderId = UUID.fromString(req("orderId") ?: return null),
                    userId = UUID.fromString(req("userId") ?: return null)
                )
                "BalanceChanged" -> BalanceChanged(
                    id = id,
                    aggregateId = aggregateId,
                    timestamp = timestamp,
                    version = version,
                    currency = org.example.domain.Currency.valueOf(req("currency") ?: return null),
                    amount = BigDecimal(req("amount") ?: return null),
                    reason = params["reason"] ?: ""
                )
                "BalanceLocked" -> BalanceLocked(
                    id = id,
                    aggregateId = aggregateId,
                    timestamp = timestamp,
                    version = version,
                    currency = org.example.domain.Currency.valueOf(req("currency") ?: return null),
                    amount = BigDecimal(req("amount") ?: return null)
                )
                "BalanceUnlocked" -> BalanceUnlocked(
                    id = id,
                    aggregateId = aggregateId,
                    timestamp = timestamp,
                    version = version,
                    currency = org.example.domain.Currency.valueOf(req("currency") ?: return null),
                    amount = BigDecimal(req("amount") ?: return null)
                )
                else -> null
            }
        }.getOrElse { e ->
            logger.warn("Failed to deserialize event from line: {}", line, e)
            null
        }
    }

    private fun parsePayload(payload: String): Map<String, String> {
        if (payload.isEmpty()) return emptyMap()
        return payload.split("&").associate {
            val parts = it.split("=", limit = 2)
            parts[0] to parts.getOrElse(1) { "" }
        }
    }
}
