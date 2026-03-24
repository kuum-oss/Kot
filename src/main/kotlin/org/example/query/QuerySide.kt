package org.example.query

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.example.domain.Side
import org.example.events.Event
import org.example.events.OrderMatched
import org.example.events.OrderPlaced
import org.example.events.OrderCancelled
import org.example.store.EventStore
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.ConcurrentHashMap

data class OrderBookView(
    val bids: List<Pair<BigDecimal, BigDecimal>>, // price to total quantity
    val asks: List<Pair<BigDecimal, BigDecimal>>
)

data class TradeView(
    val price: BigDecimal,
    val quantity: BigDecimal,
    val side: Side,
    val timestamp: Long
)

class QuerySide(
    private val eventStore: EventStore,
    private val scope: CoroutineScope
) {
    private val _orderBooks = ConcurrentHashMap<String, MutableStateFlow<OrderBookView>>()
    private val _trades = ConcurrentHashMap<String, MutableStateFlow<List<TradeView>>>()
    private val _userBalances = ConcurrentHashMap<UUID, MutableStateFlow<Map<String, BigDecimal>>>()

    init {
        scope.launch {
            eventStore.allEvents().onEach { applyEvent(it) }.collect()
        }
    }

    private fun applyEvent(event: Event) {
        when (event) {
            is OrderPlaced -> {
                // Update views
            }
            is OrderMatched -> {
                updateTrades(event)
            }
            is OrderCancelled -> {
                // Update views
            }
            else -> {} // Handle balance events if needed
        }
    }

    private fun updateTrades(event: OrderMatched) {
        val instrumentTrades = _trades.getOrPut(event.aggregateId) {
            MutableStateFlow(emptyList())
        }
        val newTrade = TradeView(event.price, event.quantity, event.side, event.timestamp.toEpochMilli())
        instrumentTrades.value = (listOf(newTrade) + instrumentTrades.value).take(100)
    }

    fun getOrderBook(instrumentId: String) = _orderBooks[instrumentId]?.asStateFlow()
    fun getTrades(instrumentId: String) = _trades[instrumentId]?.asStateFlow()
    fun getUserBalances(userId: UUID) = _userBalances[userId]?.asStateFlow()
}
