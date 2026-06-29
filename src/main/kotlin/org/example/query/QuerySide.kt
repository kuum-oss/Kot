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
import org.example.events.BalanceChanged
import org.example.store.EventStore
import org.example.engine.OrderBook
import org.example.engine.currencyBase
import org.example.engine.currencyQuote
import java.math.BigDecimal
import java.util.UUID
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

    private val orderBooksInternal = ConcurrentHashMap<String, OrderBook>()

    init {
        scope.launch {
            eventStore.allEvents().onEach { applyEvent(it) }.collect()
        }
    }

    private fun applyEvent(event: Event) {
        when (event) {
            is OrderPlaced -> {
                updateOrderBook(event.aggregateId, event)
            }
            is OrderMatched -> {
                updateTrades(event)
                updateOrderBook(event.aggregateId, event)
                updateUserBalancesForMatch(event)
            }
            is OrderCancelled -> {
                updateOrderBook(event.aggregateId, event)
            }
            is BalanceChanged -> {
                updateBalance(UUID.fromString(event.aggregateId), event.currency.name, event.amount)
            }
            else -> {} // Handle balance lock/unlock events if needed
        }
    }

    private fun updateOrderBook(instrumentId: String, event: Event) {
        val book = orderBooksInternal.getOrPut(instrumentId) { OrderBook(instrumentId) }
        book.applyEvent(event)
        
        val flow = _orderBooks.getOrPut(instrumentId) {
            MutableStateFlow(OrderBookView(emptyList(), emptyList()))
        }
        
        val bidsList = book.getBids().map { (price, orders) ->
            price to orders.sumOf { it.remainingQuantity }
        }
        val asksList = book.getAsks().map { (price, orders) ->
            price to orders.sumOf { it.remainingQuantity }
        }
        flow.value = OrderBookView(bidsList, asksList)
    }

    private fun updateTrades(event: OrderMatched) {
        val instrumentTrades = _trades.getOrPut(event.aggregateId) {
            MutableStateFlow(emptyList())
        }
        val newTrade = TradeView(event.price, event.quantity, event.side, event.timestamp.toEpochMilli())
        instrumentTrades.value = (listOf(newTrade) + instrumentTrades.value).take(100)
    }

    private fun updateBalance(userId: UUID, currency: String, delta: BigDecimal) {
        val userFlow = _userBalances.getOrPut(userId) {
            MutableStateFlow(emptyMap())
        }
        val currentMap = userFlow.value
        val currentVal = currentMap[currency] ?: BigDecimal.ZERO
        userFlow.value = currentMap + (currency to (currentVal + delta))
    }

    private fun updateUserBalancesForMatch(event: OrderMatched) {
        val base = event.currencyBase().name
        val quote = event.currencyQuote().name
        val totalQuote = event.price * event.quantity
        
        if (event.side == Side.BUY) { // Taker is BUY (USD -> BTC)
            // Taker
            updateBalance(event.takerUserId, quote, -totalQuote)
            updateBalance(event.takerUserId, base, event.quantity)
            // Maker
            updateBalance(event.makerUserId, base, -event.quantity)
            updateBalance(event.makerUserId, quote, totalQuote)
        } else { // Taker is SELL (BTC -> USD)
            // Taker
            updateBalance(event.takerUserId, base, -event.quantity)
            updateBalance(event.takerUserId, quote, totalQuote)
            // Maker
            updateBalance(event.makerUserId, quote, -totalQuote)
            updateBalance(event.makerUserId, base, event.quantity)
        }
    }

    fun getOrderBook(instrumentId: String) = _orderBooks[instrumentId]?.asStateFlow()
    fun getTrades(instrumentId: String) = _trades[instrumentId]?.asStateFlow()
    fun getUserBalances(userId: UUID) = _userBalances[userId]?.asStateFlow()
}
