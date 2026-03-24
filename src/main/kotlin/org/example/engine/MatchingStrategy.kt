package org.example.engine

import org.example.domain.Order
import org.example.events.Event
import java.time.Instant

interface MatchingStrategy {
    fun match(order: Order, book: OrderBook, timestamp: Instant): List<Event>
}

class PriceTimePriorityStrategy : MatchingStrategy {
    override fun match(order: Order, book: OrderBook, timestamp: Instant): List<Event> {
        return book.match(order, timestamp)
    }
}
