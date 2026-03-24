package org.example.events

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

interface EventUpcaster {
    fun upcast(event: Event): Event
}

class DefaultEventUpcaster : EventUpcaster {
    override fun upcast(event: Event): Event {
        // Here we could handle migrations, e.g. v1 OrderPlaced -> v2 OrderPlaced with fee
        return event
    }
}
