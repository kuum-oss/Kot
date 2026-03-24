package org.example.engine

import org.example.domain.Order
import org.example.domain.Side
import org.example.events.OrderMatched
import org.example.events.OrderPlaced
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant
import java.util.*

class MatchingTest {

    @Test
    fun `should match multiple orders and follow price-time priority`() {
        val instrumentId = "BTC/USD"
        val orderBook = OrderBook(instrumentId)

        // 1. BUY 100 @ 10
        val buy100at10 = Order(UUID.randomUUID(), UUID.randomUUID(), Side.BUY, BigDecimal("10"), BigDecimal("100"), Instant.now())
        val buyEvents = orderBook.match(buy100at10)
        assertEquals(1, buyEvents.size)
        assertEquals(OrderPlaced::class, buyEvents[0]::class)
        buyEvents.forEach { orderBook.applyEvent(it) }

        // 2. SELL 50 @ 9 (should match)
        val sell50at9 = Order(UUID.randomUUID(), UUID.randomUUID(), Side.SELL, BigDecimal("9"), BigDecimal("50"), Instant.now())
        val match1Events = orderBook.match(sell50at9)
        // Should contain OrderPlaced for Taker AND OrderMatched
        assertEquals(2, match1Events.size)
        val match1 = match1Events.find { it is OrderMatched } as OrderMatched
        assertEquals(BigDecimal("50"), match1.quantity)
        assertEquals(BigDecimal("10"), match1.price) // Maker price (10) should be used
        match1Events.forEach { orderBook.applyEvent(it) }

        // 3. SELL 50 @ 10 (should match remaining)
        val sell50at10 = Order(UUID.randomUUID(), UUID.randomUUID(), Side.SELL, BigDecimal("10"), BigDecimal("50"), Instant.now())
        val match2Events = orderBook.match(sell50at10)
        assertEquals(2, match2Events.size)
        val match2 = match2Events.find { it is OrderMatched } as OrderMatched
        assertEquals(BigDecimal("50"), match2.quantity)
        assertEquals(BigDecimal("10"), match2.price)
        match2Events.forEach { orderBook.applyEvent(it) }

        // Order Book should be empty
        assertEquals(0, orderBook.getBids().size)
        assertEquals(0, orderBook.getAsks().size)
    }

    @Test
    fun `should follow price priority for asks`() {
         val orderBook = OrderBook("TEST")
         
         // Sell 10 @ 100
         val sell1 = Order(UUID.randomUUID(), UUID.randomUUID(), Side.SELL, BigDecimal("100"), BigDecimal("10"), Instant.now())
         orderBook.match(sell1).forEach { orderBook.applyEvent(it) }
         
         // Sell 10 @ 90
         val sell2 = Order(UUID.randomUUID(), UUID.randomUUID(), Side.SELL, BigDecimal("90"), BigDecimal("10"), Instant.now())
         orderBook.match(sell2).forEach { orderBook.applyEvent(it) }
         
         // Buy 15 @ 110. Should match 10@90 first, then 5@100
         val buy = Order(UUID.randomUUID(), UUID.randomUUID(), Side.BUY, BigDecimal("110"), BigDecimal("15"), Instant.now())
         val matches = orderBook.match(buy).filterIsInstance<OrderMatched>()
         
         assertEquals(2, matches.size)
         assertEquals(BigDecimal("90"), matches[0].price)
         assertEquals(BigDecimal("10"), matches[0].quantity)
         
         assertEquals(BigDecimal("100"), matches[1].price)
         assertEquals(BigDecimal("5"), matches[1].quantity)
    }
}
