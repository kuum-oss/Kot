package org.example.engine

import org.example.domain.Order
import org.example.domain.Side
import org.example.events.Event
import org.example.events.OrderMatched
import org.example.events.OrderPlaced
import org.example.events.OrderCancelled
import java.math.BigDecimal
import java.time.Instant
import java.util.*

class OrderBook(val instrumentId: String) {
    // Bids sorted by price DESC, then timestamp ASC
    private val bids = TreeMap<BigDecimal, MutableList<Order>> { p1, p2 -> p2.compareTo(p1) }
    // Asks sorted by price ASC, then timestamp ASC
    private val asks = TreeMap<BigDecimal, MutableList<Order>> { p1, p2 -> p1.compareTo(p2) }

    private val orderMap = mutableMapOf<UUID, Order>()

    fun applyEvent(event: Event) {
        when (event) {
            is OrderPlaced -> {
                val order = Order(
                    id = event.orderId,
                    userId = event.userId,
                    side = event.side,
                    price = event.price,
                    quantity = event.quantity,
                    timestamp = event.timestamp
                )
                addOrder(order)
            }
            is OrderMatched -> {
                updateOrderFill(event.makerOrderId, event.quantity)
                updateOrderFill(event.takerOrderId, event.quantity)
            }
            is OrderCancelled -> {
                removeOrder(event.orderId)
            }
            else -> {} // Ignore balance events in OrderBook
        }
    }

    private fun addOrder(order: Order) {
        orderMap[order.id] = order
        val book = if (order.side == Side.BUY) bids else asks
        book.getOrPut(order.price) { mutableListOf() }.add(order)
    }

    private fun updateOrderFill(orderId: UUID, filledQuantity: BigDecimal) {
        val order = orderMap[orderId] ?: return
        val updatedOrder = order.copy(filledQuantity = order.filledQuantity + filledQuantity)
        
        if (updatedOrder.isFilled) {
            removeOrder(orderId)
        } else {
            // Update in place if it's still there
            val book = if (order.side == Side.BUY) bids else asks
            val list = book[order.price]
            if (list != null) {
                val index = list.indexOfFirst { it.id == orderId }
                if (index != -1) {
                    list[index] = updatedOrder
                    orderMap[orderId] = updatedOrder
                }
            }
        }
    }

    private fun removeOrder(orderId: UUID) {
        val order = orderMap.remove(orderId) ?: return
        val book = if (order.side == Side.BUY) bids else asks
        val list = book[order.price]
        if (list != null) {
            list.removeIf { it.id == orderId }
            if (list.isEmpty()) {
                book.remove(order.price)
            }
        }
    }

    fun match(takerOrder: Order, timestamp: Instant): List<Event> {
        val events = mutableListOf<Event>()
        val oppositeBook = if (takerOrder.side == Side.BUY) asks else bids
        var remainingTakerQty = takerOrder.quantity

        val it = oppositeBook.entries.iterator()
        while (it.hasNext() && remainingTakerQty > BigDecimal.ZERO) {
            val entry = it.next()
            val price = entry.key

            // Check if price matches
            val canMatch = if (takerOrder.side == Side.BUY) {
                price <= takerOrder.price
            } else {
                price >= takerOrder.price
            }

            if (!canMatch) break

            val makerList = entry.value
            val makerIt = makerList.iterator()
            while (makerIt.hasNext() && remainingTakerQty > BigDecimal.ZERO) {
                val makerOrder = makerIt.next()
                val matchQty = makerOrder.remainingQuantity.min(remainingTakerQty)

                events.add(OrderMatched(
                    aggregateId = instrumentId,
                    timestamp = timestamp,
                    makerOrderId = makerOrder.id,
                    takerOrderId = takerOrder.id,
                    makerUserId = makerOrder.userId,
                    takerUserId = takerOrder.userId,
                    price = price,
                    quantity = matchQty,
                    side = takerOrder.side
                ))

                remainingTakerQty -= matchQty
            }
        }

        // Always emit OrderPlaced for the taker order
        events.add(0, OrderPlaced(
            aggregateId = instrumentId,
            timestamp = timestamp,
            orderId = takerOrder.id,
            userId = takerOrder.userId,
            side = takerOrder.side,
            price = takerOrder.price,
            quantity = takerOrder.quantity
        ))

        return events
    }

    // Getters for Query side
    fun getBids() = bids.mapValues { it.value.toList() }
    fun getAsks() = asks.mapValues { it.value.toList() }
    fun getOrder(id: UUID) = orderMap[id]
}
