package org.example

import kotlinx.coroutines.*
import org.example.cluster.ClusterNode
import org.example.cluster.NodeRole
import org.example.commands.PlaceOrder
import org.example.domain.Currency
import org.example.domain.Side
import org.example.events.BalanceChanged
import org.example.observability.MetricsCollector
import org.example.security.SecurityGateway
import java.math.BigDecimal
import java.time.Instant
import java.util.*
import kotlin.random.Random

fun main() = runBlocking {
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val security = SecurityGateway()
    
    // 1. Setup Cluster
    val node1 = ClusterNode("Node-1", scope)
    val node2 = ClusterNode("Node-2", scope)
    val node3 = ClusterNode("Node-3", scope)
    
    val nodes = listOf(node1, node2, node3)
    nodes.forEach { it.setPeers(nodes.filter { p -> p != it }) }
    
    // Node-1 is the Leader
    node1.role = NodeRole.LEADER
    
    // Start replication loops
    nodes.forEach { scope.launch { it.startReplicationLoop() } }
    
    println("🚀 Cluster started with 3 nodes. Leader is Node-1.")
    
    // 2. Initialize balances (Seed)
    val userIds = List(5) { UUID.randomUUID() }
    val initialEvents = userIds.flatMap { userId ->
        listOf(
            BalanceChanged(
                aggregateId = userId.toString(),
                timestamp = Instant.now(),
                currency = Currency.USD,
                amount = BigDecimal("100000"),
                reason = "Initial deposit"
            ),
            BalanceChanged(
                aggregateId = userId.toString(),
                timestamp = Instant.now(),
                currency = Currency.BTC,
                amount = BigDecimal("10"),
                reason = "Initial deposit"
            )
        )
    }
    initialEvents.forEach { node1.balanceService.applyEvent(it) }
    node1.eventStore.append("SYSTEM", 0, initialEvents)

    // 3. Simulation with Chaos
    val instruments = listOf("BTC/USD")
    
    val simulationJob = scope.launch {
        repeat(1000) { i ->
            val userId = userIds.random()
            
            // Security: Auth & Rate limiting
            val authId = security.authenticate("token-$userId")
            if (authId != null && security.checkRateLimit(authId)) {
                val command = PlaceOrder(
                    instrumentId = instruments.random(),
                    userId = userId, // Use the original userId which has the balance
                    side = if (Random.nextBoolean()) Side.BUY else Side.SELL,
                    price = BigDecimal(90000 + Random.nextInt(2000)),
                    quantity = BigDecimal(1 + Random.nextInt(5))
                )
                
                // Process on current leader
                val leader = nodes.find { it.role == NodeRole.LEADER }
                leader?.processCommand(command)
            }
            
            if (i % 100 == 0) {
                MetricsCollector.report()
                delay(100)
            }
            
            // Chaos: Simulate node failure / role switch
            if (i == 500) {
                println("🔥 Chaos: Switching Leader to Node-2")
                node1.role = NodeRole.FOLLOWER
                node2.role = NodeRole.LEADER
            }
        }
    }

    delay(5000) // Wait for simulation to finish
    simulationJob.cancel()
    
    println("✅ Simulation finished.")
    MetricsCollector.report()
    
    // Consistency check: Verify balances on all nodes
    println("\n📊 Consistency Check:")
    userIds.take(2).forEach { userId ->
        println("User $userId:")
        nodes.forEach { node ->
            val usd = node.balanceService.getAccount(userId, Currency.USD).total
            val btc = node.balanceService.getAccount(userId, Currency.BTC).total
            println("  Node ${node.nodeId}: USD=$usd, BTC=$btc")
        }
    }
    
    scope.cancel()
}