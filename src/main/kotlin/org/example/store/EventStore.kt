package org.example.store

import org.example.events.Event
import org.example.events.EventUpcaster
import org.example.events.DefaultEventUpcaster
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.map
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class OptimisticLockException(message: String) : Exception(message)

interface EventStore {
    suspend fun append(aggregateId: String, expectedVersion: Int, events: List<Event>)
    suspend fun appendReplicated(events: List<Event>)
    fun getStream(aggregateId: String): List<Event>
    fun allEvents(): Flow<Event>
    fun getCurrentVersion(aggregateId: String): Int
    suspend fun subscribePersistence(persistence: FilePersistence)
}

class InMemoryEventStore(
    private val upcaster: EventUpcaster = DefaultEventUpcaster()
) : EventStore {
    private val streams = ConcurrentHashMap<String, MutableList<Event>>()
    private val versions = ConcurrentHashMap<String, AtomicInteger>()
    private val _allEventsFlow = MutableSharedFlow<Event>(replay = 1000)
    private var persistence: FilePersistence? = null
    private val processedIds = ConcurrentHashMap.newKeySet<java.util.UUID>()

    override suspend fun append(aggregateId: String, expectedVersion: Int, events: List<Event>) {
        val stream = streams.getOrPut(aggregateId) { mutableListOf() }
        val currentVersion = versions.getOrPut(aggregateId) { AtomicInteger(0) }

        synchronized(stream) {
            if (currentVersion.get() != expectedVersion) {
                throw OptimisticLockException(
                    "Version mismatch for $aggregateId: expected $expectedVersion, but got ${currentVersion.get()}"
                )
            }

            val newEvents = events.filter { processedIds.add(it.id) }
            stream.addAll(newEvents)
            currentVersion.addAndGet(newEvents.size)
            
            // Re-emit outside sync or keep it inside for order? 
            // In a simple system, we can emit outside.
        }

        events.forEach { 
            _allEventsFlow.emit(it)
            persistence?.persist(it)
        }
    }

    override suspend fun appendReplicated(events: List<Event>) {
        events.forEach { event ->
            if (processedIds.add(event.id)) {
                val stream = streams.getOrPut(event.aggregateId) { mutableListOf() }
                val currentVersion = versions.getOrPut(event.aggregateId) { AtomicInteger(0) }
                synchronized(stream) {
                    stream.add(event)
                    currentVersion.incrementAndGet()
                }
                _allEventsFlow.emit(event)
                persistence?.persist(event)
            }
        }
    }

    override suspend fun subscribePersistence(persistence: FilePersistence) {
        this.persistence = persistence
        val persistedEvents = persistence.loadEvents()
        persistedEvents.forEach { event ->
            if (processedIds.add(event.id)) {
                val stream = streams.getOrPut(event.aggregateId) { mutableListOf() }
                val currentVersion = versions.getOrPut(event.aggregateId) { AtomicInteger(0) }
                synchronized(stream) {
                    stream.add(event)
                    currentVersion.incrementAndGet()
                }
                _allEventsFlow.emit(event)
            }
        }
    }

    override fun getStream(aggregateId: String): List<Event> {
        return (streams[aggregateId]?.toList() ?: emptyList()).map { upcaster.upcast(it) }
    }

    override fun allEvents(): Flow<Event> = _allEventsFlow.asSharedFlow().map { upcaster.upcast(it) }

    override fun getCurrentVersion(aggregateId: String): Int {
        return versions[aggregateId]?.get() ?: 0
    }
}
