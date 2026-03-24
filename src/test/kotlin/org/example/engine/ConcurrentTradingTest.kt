package org.example.engine

import kotlinx.coroutines.*
import org.example.commands.PlaceOrder
import org.example.domain.Side
import org.example.store.InMemoryEventStore
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class ConcurrentTradingTest {

    @Test
    fun `should handle concurrent orders for same instrument`() = runBlocking {
        val eventStore = InMemoryEventStore()
        val balanceService = BalanceService()
        val scope = CoroutineScope(Dispatchers.Default + Job())
        val engine = TradingEngine(eventStore, balanceService, scope)
        val instrumentId = "BTC/USD"
        
        val numUsers = 100
        val ordersPerUser = 10
        val totalOrders = numUsers * ordersPerUser
        
        val counter = AtomicInteger(0)
        
        val jobs = List(numUsers) { userId ->
            val uId = UUID.randomUUID()
            // Initial balance for user
            balanceService.applyEvent(org.example.events.BalanceChanged(
                aggregateId = uId.toString(),
                timestamp = java.time.Instant.now(),
                currency = org.example.domain.Currency.USD,
                amount = BigDecimal("1000000"),
                reason = "test"
            ))
            balanceService.applyEvent(org.example.events.BalanceChanged(
                aggregateId = uId.toString(),
                timestamp = java.time.Instant.now(),
                currency = org.example.domain.Currency.BTC,
                amount = BigDecimal("1000"),
                reason = "test"
            ))

            launch {
                repeat(ordersPerUser) {
                    val side = if (it % 2 == 0) Side.BUY else Side.SELL
                    engine.process(PlaceOrder(
                        instrumentId = instrumentId,
                        userId = uId,
                        side = side,
                        price = BigDecimal("100"),
                        quantity = BigDecimal("1")
                    ))
                    counter.incrementAndGet()
                }
            }
        }
        
        jobs.joinAll()
        
        // Wait for actor to finish processing
        // Since we use UNLIMITED channel and actors process sequentially, 
        // we need a way to know it's done.
        // For test, we can just wait or check event store size.
        
        var attempts = 0
        while (eventStore.getStream(instrumentId).filterIsInstance<org.example.events.OrderPlaced>().size < totalOrders && attempts < 50) {
            delay(100)
            attempts++
        }
        
        val placedEvents = eventStore.getStream(instrumentId).filterIsInstance<org.example.events.OrderPlaced>()
        assertEquals(totalOrders, placedEvents.size, "All orders should be placed")
        
        scope.cancel()
    }
}
