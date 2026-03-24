package org.example.engine

import org.example.domain.Account
import org.example.domain.Currency
import org.example.events.*
import java.math.BigDecimal
import java.util.UUID

class BalanceService {
    private val accounts = mutableMapOf<UUID, MutableMap<Currency, Account>>()
    private val processedEventIds = mutableSetOf<UUID>()

    fun applyEvent(event: Event) {
        if (!processedEventIds.add(event.id)) return
        println("[DEBUG_LOG] Node applying event: $event")
        when (event) {
            is BalanceChanged -> {
                val userId = UUID.fromString(event.aggregateId)
                updateAccount(userId, event.currency, event.amount, BigDecimal.ZERO)
            }
            is BalanceLocked -> {
                val userId = UUID.fromString(event.aggregateId)
                updateAccount(userId, event.currency, -event.amount, event.amount)
            }
            is BalanceUnlocked -> {
                val userId = UUID.fromString(event.aggregateId)
                updateAccount(userId, event.currency, event.amount, -event.amount)
            }
            is OrderPlaced -> {
                // If taker, funds are locked before the event is even appended to store? 
                // Actually, in ES, OrderPlaced might imply funds were already checked and locked.
                // Let's assume funds are locked by BalanceLocked event BEFORE OrderPlaced, OR OrderPlaced handles locking.
                // Given the requirements, I'll have OrderPlaced handle locking if it's the first event.
            }
            is OrderMatched -> {
                val base = event.currencyBase()
                val quote = event.currencyQuote()
                val totalQuote = event.price * event.quantity
                
                if (event.side == org.example.domain.Side.BUY) { // Taker is BUY (USD -> BTC)
                    // Taker
                    updateAccount(event.takerUserId, quote, BigDecimal.ZERO, -totalQuote)
                    updateAccount(event.takerUserId, base, event.quantity, BigDecimal.ZERO)
                    // Maker
                    updateAccount(event.makerUserId, base, BigDecimal.ZERO, -event.quantity)
                    updateAccount(event.makerUserId, quote, totalQuote, BigDecimal.ZERO)
                } else { // Taker is SELL (BTC -> USD)
                    // Taker
                    updateAccount(event.takerUserId, base, BigDecimal.ZERO, -event.quantity)
                    updateAccount(event.takerUserId, quote, totalQuote, BigDecimal.ZERO)
                    // Maker
                    updateAccount(event.makerUserId, quote, BigDecimal.ZERO, -totalQuote)
                    updateAccount(event.makerUserId, base, event.quantity, BigDecimal.ZERO)
                }
            }
            is OrderCancelled -> {
                // Should be handled by BalanceUnlocked separately or here
            }
        }
    }

    private fun updateAccount(userId: UUID, currency: Currency, availDelta: BigDecimal, lockedDelta: BigDecimal) {
        val userAccounts = accounts.getOrPut(userId) { mutableMapOf() }
        val account = userAccounts.getOrPut(currency) { Account(userId, currency) }
        userAccounts[currency] = account.copy(
            available = account.available + availDelta,
            locked = account.locked + lockedDelta
        )
    }

    fun getAccount(userId: UUID, currency: Currency): Account {
        return accounts[userId]?.get(currency) ?: Account(userId, currency)
    }

    // Double-entry check
    fun getTotalBalance(currency: Currency): BigDecimal {
        return accounts.values.sumOf { it[currency]?.total ?: BigDecimal.ZERO }
    }
}

// Helper to deduce currencies from aggregateId (e.g., "BTC/USD")
fun Event.currencyBase(): Currency = Currency.valueOf(aggregateId.split("/")[0])
fun Event.currencyQuote(): Currency = Currency.valueOf(aggregateId.split("/")[1])
