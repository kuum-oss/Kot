package org.example.engine

import kotlinx.coroutines.*
import org.example.commands.PlaceOrder
import org.example.domain.Side
import org.example.store.InMemoryEventStore
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.*

class ReplayTest {

    @Test
    fun `should restore order book state from events`() = runBlocking {
        val eventStore = InMemoryEventStore()
        val instrumentId = "REPLAY-BTC"
        
        // 1. Manually append some events
        val userId = UUID.randomUUID()
        val order1Id = UUID.randomUUID()
        val events = listOf(
            org.example.events.OrderPlaced(
                aggregateId = instrumentId,
                orderId = order1Id,
                userId = userId,
                side = Side.BUY,
                price = BigDecimal("10000"),
                quantity = BigDecimal("1")
            )
        )
        eventStore.append(instrumentId, 0, events)
        
        // 2. Start engine - it should replay events
        val scope = CoroutineScope(Dispatchers.Default + Job())
        val actor = MatchingActor(instrumentId, eventStore, scope)
        
        // Since replay is in init (synchronous for OrderBook, but actor loop starts after),
        // we can check if it already matched another order.
        
        // Place a matching sell order
        actor.send(PlaceOrder(
            instrumentId = instrumentId,
            userId = UUID.randomUUID(),
            side = Side.SELL,
            price = BigDecimal("10000"),
            quantity = BigDecimal("1")
        ))
        
        // Wait for processing
        var attempts = 0
        while (eventStore.getStream(instrumentId).size < 3 && attempts < 20) {
            delay(100)
            attempts++
        }
        
        val stream = eventStore.getStream(instrumentId)
        // Expected: OrderPlaced (1st), OrderPlaced (2nd), OrderMatched
        assertEquals(3, stream.size)
        assert(stream.any { it is org.example.events.OrderMatched })
        
        scope.cancel()
    }
}
