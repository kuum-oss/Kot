package org.example.domain

import java.math.BigDecimal
import java.util.UUID
import java.time.Instant

enum class Side {
    BUY, SELL
}

data class Order(
    val id: UUID,
    val userId: UUID,
    val side: Side,
    val price: BigDecimal,
    val quantity: BigDecimal,
    val timestamp: Instant,
    val filledQuantity: BigDecimal = BigDecimal.ZERO
) {
    val remainingQuantity: BigDecimal get() = quantity - filledQuantity
    val isFilled: Boolean get() = remainingQuantity <= BigDecimal.ZERO
}

data class Trade(
    val makerOrderId: UUID,
    val takerOrderId: UUID,
    val side: Side, // Taker side
    val price: BigDecimal,
    val quantity: BigDecimal,
    val timestamp: Instant
)

enum class Currency {
    USD, BTC, ETH
}

data class Account(
    val userId: UUID,
    val currency: Currency,
    val available: BigDecimal = BigDecimal.ZERO,
    val locked: BigDecimal = BigDecimal.ZERO
) {
    val total: BigDecimal get() = available + locked
}
