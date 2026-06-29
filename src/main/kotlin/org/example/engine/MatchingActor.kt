package org.example.engine

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.example.commands.*
import org.example.domain.Currency
import org.example.domain.Order
import org.example.events.*
import org.example.observability.MetricsCollector
import org.example.store.EventStore
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

/** Internal message envelope for the actor channel. */
private sealed interface ActorMessage {
    data class Cmd(val command: Command) : ActorMessage
    data class Drain(val latch: CompletableDeferred<Unit>) : ActorMessage
}

class MatchingActor(
    val instrumentId: String,
    private val eventStore: EventStore,
    private val balanceService: BalanceService,
    private val strategy: OverloadStrategy = OverloadStrategy.Backpressure,
    scope: CoroutineScope
) {
    private val logger = LoggerFactory.getLogger(MatchingActor::class.java)
    private val actorChannel: Channel<ActorMessage> = when (strategy) {
        is OverloadStrategy.Drop -> Channel(1000)
        is OverloadStrategy.Backpressure -> Channel(Channel.BUFFERED)
        is OverloadStrategy.Degrade -> Channel(Channel.UNLIMITED)
    }
    private val orderBook = OrderBook(instrumentId)
    private val idempotencyRegistry = mutableSetOf<UUID>()
    private val matchingStrategy: MatchingStrategy = PriceTimePriorityStrategy()

    init {
        // Replay
        val events = eventStore.getStream(instrumentId)
        events.forEach { 
            orderBook.applyEvent(it)
            balanceService.applyEvent(it)
        }
        
        scope.launch {
            for (msg in actorChannel) {
                when (msg) {
                    is ActorMessage.Drain -> msg.latch.complete(Unit)
                    is ActorMessage.Cmd -> {
                        val command = msg.command
                        try {
                            if (idempotencyRegistry.contains(command.id)) {
                                logger.warn("Duplicate command: {}", command.id)
                                continue
                            }
                            handleCommand(command)
                            idempotencyRegistry.add(command.id)
                        } catch (e: Exception) {
                            logger.error("Error handling command: {}", command, e)
                        }
                    }
                }
            }
        }
    }

    suspend fun send(command: Command): Boolean {
        return when (strategy) {
            is OverloadStrategy.Drop -> {
                val success = actorChannel.trySend(ActorMessage.Cmd(command)).isSuccess
                if (!success) logger.warn("Command dropped due to overload: {}", command.id)
                success
            }
            else -> {
                actorChannel.send(ActorMessage.Cmd(command))
                true
            }
        }
    }

    /**
     * Sends a drain sentinel through the same channel and suspends until the actor
     * has processed all previously enqueued commands. Use this in tests instead of
     * polling with delay() for deterministic synchronization.
     */
    suspend fun drain() {
        val latch = CompletableDeferred<Unit>()
        actorChannel.send(ActorMessage.Drain(latch))
        latch.await()
    }

    private suspend fun handleCommand(command: Command) {
        val startTime = System.nanoTime()
        val currentVersion = eventStore.getCurrentVersion(instrumentId)
        val events = when (command) {
            is PlaceOrder -> {
                // 1. Check Balance (Deterministic)
                val base = Currency.valueOf(instrumentId.split("/")[0])
                val quote = Currency.valueOf(instrumentId.split("/")[1])
                
                val (currency, requiredAmount) = if (command.side == org.example.domain.Side.BUY) {
                    quote to command.price * command.quantity
                } else {
                    base to command.quantity
                }
                
                val account = balanceService.getAccount(command.userId, currency)
                if (account.available < requiredAmount) {
                    logger.warn("Insufficient funds for user {}: needed {}, has {}", command.userId, requiredAmount, account.available)
                    return
                }

                // 2. Prepare Events
                val takerOrder = Order(
                    id = command.orderId,
                    userId = command.userId,
                    side = command.side,
                    price = command.price,
                    quantity = command.quantity,
                    timestamp = command.timestamp
                )
                
                // Add BalanceLocked event
                val lockEvent = BalanceLocked(
                    aggregateId = command.userId.toString(),
                    timestamp = command.timestamp,
                    currency = currency,
                    amount = requiredAmount
                )
                
                val matchEvents = matchingStrategy.match(takerOrder, orderBook, command.timestamp)
                
                listOf(lockEvent) + matchEvents
            }
            is CancelOrder -> {
                val order = orderBook.getOrder(command.orderId)
                if (order != null && order.userId == command.userId) {
                    // Unlock funds logic would be here
                    val base = Currency.valueOf(instrumentId.split("/")[0])
                    val quote = Currency.valueOf(instrumentId.split("/")[1])
                    val (currency, amount) = if (order.side == org.example.domain.Side.BUY) {
                        quote to order.remainingQuantity * order.price
                    } else {
                        base to order.remainingQuantity
                    }
                    
                    val cancelEvent = OrderCancelled(
                        aggregateId = instrumentId,
                        timestamp = command.timestamp,
                        orderId = command.orderId,
                        userId = command.userId
                    )
                    
                    val unlockEvent = BalanceUnlocked(
                        aggregateId = command.userId.toString(),
                        timestamp = command.timestamp,
                        currency = currency,
                        amount = amount
                    )
                    
                    listOf(cancelEvent, unlockEvent)
                } else {
                    emptyList()
                }
            }
        }

        if (events.isNotEmpty()) {
            eventStore.append(instrumentId, currentVersion, events)
            events.forEach { 
                orderBook.applyEvent(it)
                balanceService.applyEvent(it)
                if (it is OrderMatched) MetricsCollector.totalMatches.incrementAndGet()
            }
        }
        
        MetricsCollector.totalCommands.incrementAndGet()
        MetricsCollector.recordLatency(System.nanoTime() - startTime)
    }
}
