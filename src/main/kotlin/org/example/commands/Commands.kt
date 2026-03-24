package org.example.commands

import org.example.domain.Side
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

sealed interface Command {
    val id: UUID
    val instrumentId: String
    val timestamp: Instant
}

data class PlaceOrder(
    override val id: UUID = UUID.randomUUID(),
    override val instrumentId: String,
    val userId: UUID,
    val side: Side,
    val price: BigDecimal,
    val quantity: BigDecimal,
    override val timestamp: Instant = Instant.now(),
    val orderId: UUID = id
) : Command

data class CancelOrder(
    override val id: UUID = UUID.randomUUID(),
    override val instrumentId: String,
    val userId: UUID,
    val orderId: UUID,
    override val timestamp: Instant = Instant.now()
) : Command

sealed class OverloadStrategy {
    data object Drop : OverloadStrategy()
    data object Backpressure : OverloadStrategy()
    data object Degrade : OverloadStrategy()
}
