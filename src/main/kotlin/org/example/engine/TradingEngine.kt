package org.example.engine

import kotlinx.coroutines.CoroutineScope
import org.example.commands.Command
import org.example.store.EventStore
import org.example.commands.OverloadStrategy
import java.util.concurrent.ConcurrentHashMap

class TradingEngine(
    private val eventStore: EventStore,
    private val balanceService: BalanceService,
    private val scope: CoroutineScope,
    private val overloadStrategy: OverloadStrategy = OverloadStrategy.Backpressure
) {
    private val actors = ConcurrentHashMap<String, MatchingActor>()

    suspend fun process(command: Command): Boolean {
        val actor = actors.getOrPut(command.instrumentId) {
            MatchingActor(command.instrumentId, eventStore, balanceService, overloadStrategy, scope)
        }
        return actor.send(command)
    }
}
