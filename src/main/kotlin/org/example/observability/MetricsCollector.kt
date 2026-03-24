package org.example.observability

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

object MetricsCollector {
    private val logger = LoggerFactory.getLogger(MetricsCollector::class.java)
    val totalCommands = AtomicLong(0)
    val totalMatches = AtomicLong(0)
    val latencies = ConcurrentLinkedQueue<Long>()

    fun recordLatency(nanos: Long) {
        latencies.add(nanos)
        if (latencies.size > 1000) latencies.poll()
    }

    fun report() {
        val count = totalCommands.get()
        val matches = totalMatches.get()
        val p99 = latencies.sorted().let { if (it.isNotEmpty()) it[(it.size * 0.99).toInt()] else 0 }
        logger.info("Metrics: throughput=$count cmds, matches=$matches, p99 latency=${p99 / 1_000_000.0}ms")
    }
}
