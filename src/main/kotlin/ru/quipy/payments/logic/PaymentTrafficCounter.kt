package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry

/**
 * Counts payments sent to the provider, including payments sent in bulk.
 */
class PaymentTrafficCounter(
    registry: MeterRegistry,
    accountName: String,
) {
    private val outgoing = Counter.builder("shop.payment.outgoing")
            .description("Payments sent to the provider")
            .tag("account", accountName)
            .register(registry)

    fun increment(paymentCount: Int) {
        outgoing.increment(paymentCount.toDouble())
    }
}
