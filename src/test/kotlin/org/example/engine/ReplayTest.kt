package org.example.engine

import kotlinx.coroutines.*
import org.example.commands.PlaceOrder
import org.example.domain.Side
import org.example.store.InMemoryEventStore
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant
import java.util.*

class ReplayTest {

    @Test
    fun `should restore order book state from events`() = runBlocking {
        val eventStore = InMemoryEventStore()
        val instrumentId = "BTC/USD"

        // 1. Pre-seed an existing BUY order directly into the event store
        val buyerId = UUID.randomUUID()
        val order1Id = UUID.randomUUID()
        val timestamp = Instant.now()
        val events = listOf(
            org.example.events.OrderPlaced(
                aggregateId = instrumentId,
                timestamp = timestamp,
                orderId = order1Id,
                userId = buyerId,
                side = Side.BUY,
                price = BigDecimal("10000"),
                quantity = BigDecimal("1")
            )
        )
        eventStore.append(instrumentId, 0, events)

        // 2. Start actor — it replays the existing event stream in init{}
        val balanceService = BalanceService()
        val scope = CoroutineScope(Dispatchers.Default + Job())
        val actor = MatchingActor(instrumentId, eventStore, balanceService, scope = scope)

        // 3. Give the seller a balance and send a matching SELL order
        val sellerId = UUID.randomUUID()
        balanceService.applyEvent(
            org.example.events.BalanceChanged(
                aggregateId = sellerId.toString(),
                timestamp = Instant.now(),
                currency = org.example.domain.Currency.BTC,
                amount = BigDecimal("10"),
                reason = "Test Deposit"
            )
        )

        actor.send(PlaceOrder(
            instrumentId = instrumentId,
            userId = sellerId,
            side = Side.SELL,
            price = BigDecimal("10000"),
            quantity = BigDecimal("1")
        ))

        // 4. Deterministic wait: drain flushes the sentinel through the same channel,
        //    guaranteeing the SELL command is fully processed before we assert.
        actor.drain()

        val stream = eventStore.getStream(instrumentId)
        // Expected: OrderPlaced (BUY seed), BalanceLocked, OrderPlaced (SELL), OrderMatched
        assertEquals(4, stream.size)
        assert(stream.any { it is org.example.events.OrderMatched })

        scope.cancel()
    }
}