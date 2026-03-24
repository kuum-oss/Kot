package org.example.events

import org.example.domain.Side
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

sealed interface Event {
    val id: UUID
    val aggregateId: String
    val timestamp: Instant
    val version: Int
}

data class OrderPlaced(
    override val id: UUID = UUID.randomUUID(),
    override val aggregateId: String, // Instrument ID
    override val timestamp: Instant,
    override val version: Int = 1,
    val orderId: UUID,
    val userId: UUID,
    val side: Side,
    val price: BigDecimal,
    val quantity: BigDecimal
) : Event

data class OrderMatched(
    override val id: UUID = UUID.randomUUID(),
    override val aggregateId: String,
    override val timestamp: Instant,
    override val version: Int = 1,
    val makerOrderId: UUID,
    val takerOrderId: UUID,
    val makerUserId: UUID,
    val takerUserId: UUID,
    val price: BigDecimal,
    val quantity: BigDecimal,
    val side: Side // Taker side
) : Event

data class OrderCancelled(
    override val id: UUID = UUID.randomUUID(),
    override val aggregateId: String,
    override val timestamp: Instant,
    override val version: Int = 1,
    val orderId: UUID,
    val userId: UUID
) : Event

data class BalanceChanged(
    override val id: UUID = UUID.randomUUID(),
    override val aggregateId: String, // userId
    override val timestamp: Instant,
    override val version: Int = 1,
    val currency: org.example.domain.Currency,
    val amount: BigDecimal,
    val reason: String
) : Event

data class BalanceLocked(
    override val id: UUID = UUID.randomUUID(),
    override val aggregateId: String, // userId
    override val timestamp: Instant,
    override val version: Int = 1,
    val currency: org.example.domain.Currency,
    val amount: BigDecimal
) : Event

data class BalanceUnlocked(
    override val id: UUID = UUID.randomUUID(),
    override val aggregateId: String, // userId
    override val timestamp: Instant,
    override val version: Int = 1,
    val currency: org.example.domain.Currency,
    val amount: BigDecimal
) : Event
