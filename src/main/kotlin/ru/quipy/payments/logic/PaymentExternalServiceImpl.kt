package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.Dispatcher
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        private val httpTimeout = Duration.ofMinutes(3)
        private val jsonMediaType = "application/json".toMediaType()
        private const val batchIntervalMillis = 2_000L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val parallelRequests = properties.parallelRequests

    private val traffic = PaymentTrafficCounter(
        registry = meterRegistry,
        accountName = accountName,
    )

    private val client = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(httpTimeout)
        .writeTimeout(Duration.ofSeconds(10))
        .callTimeout(httpTimeout)
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests.coerceAtLeast(1)
            maxRequestsPerHost = parallelRequests.coerceAtLeast(1)
        })
        .build()

    private val workers = ThreadPoolExecutor(
        parallelRequests.coerceIn(1, 512),
        parallelRequests.coerceIn(1, 512),
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(10_000),
        NamedThreadFactory("payment-$accountName"),
        CallerBlockingRejectedExecutionHandler(),
    )

    private val pendingPayments = LinkedBlockingQueue<PendingPayment>()
    private val batchScheduler = ScheduledThreadPoolExecutor(
        1,
        NamedThreadFactory("payment-$accountName-batcher"),
    ).apply {
        scheduleAtFixedRate(
            { flushBatch() },
            batchIntervalMillis,
            batchIntervalMillis,
            TimeUnit.MILLISECONDS,
        )
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        pendingPayments.put(PendingPayment(paymentId, amount, paymentStartedAt, deadline, UUID.randomUUID()))
    }

    private fun flushBatch() {
        try {
            val batch = mutableListOf<PendingPayment>()
            pendingPayments.drainTo(batch)
            if (batch.isNotEmpty()) {
                workers.execute {
                    processBatch(batch)
                }
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Failed to schedule payment batch", e)
        }
    }

    private fun processBatch(batch: List<PendingPayment>) {
        val latestStartReserve = requestAverageProcessingTime.multipliedBy(2).toMillis()
        val now = now()
        val (ready, expired) = batch.partition { now + latestStartReserve <= it.deadline }
        expired.forEach {
            logOutcome(it, submitted = false, success = false, reason = "deadline exceeded before submit")
        }
        if (ready.isEmpty()) return

        val submittedAt = now()
        ready.forEach { payment ->
            logSubmission(payment, submittedAt)
        }
        traffic.increment(ready.size)

        val body = BulkPaymentRequest(
            serviceName = serviceName,
            accountName = accountName,
            requests = ready.map {
                BulkPaymentItem(it.transactionId.toString(), it.paymentId.toString(), it.amount)
            },
        )
        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process/bulk")
            .put(mapper.writeValueAsBytes(body).toRequestBody(jsonMediaType))
            .build()

        try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    throw IllegalStateException("Provider returned HTTP ${response.code}: ${response.body?.string()}")
                }
                val result = mapper.readValue(response.body?.string(), BulkPaymentResponse::class.java)
                val responsesByTransaction = result.responses.associateBy { it.transactionId }
                ready.forEach { payment ->
                    val paymentResult = responsesByTransaction[payment.transactionId.toString()]
                    logProcessing(
                        payment,
                        success = paymentResult?.result == true,
                        reason = paymentResult?.message ?: if (paymentResult == null) "Missing bulk response" else null,
                    )
                }
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Bulk of ${ready.size} payments failed", e)
            ready.forEach {
                logProcessing(it, success = false, reason = e.message)
            }
        }
    }

    private fun logSubmission(payment: PendingPayment, submittedAt: Long) {
        paymentESService.update(payment.paymentId) {
            it.logSubmission(
                success = true,
                transactionId = payment.transactionId,
                startedAt = submittedAt,
                spentInQueueDuration = Duration.ofMillis(submittedAt - payment.paymentStartedAt),
            )
        }
        logger.info("[$accountName] Submit: ${payment.paymentId}, txId: ${payment.transactionId}")
    }

    private fun logProcessing(payment: PendingPayment, success: Boolean, reason: String?) {
        paymentESService.update(payment.paymentId) {
            it.logProcessing(success, now(), payment.transactionId, reason)
        }
    }

    private fun logOutcome(payment: PendingPayment, submitted: Boolean, success: Boolean, reason: String) {
        val submittedAt = now()
        paymentESService.update(payment.paymentId) {
            it.logSubmission(
                submitted,
                payment.transactionId,
                submittedAt,
                Duration.ofMillis(submittedAt - payment.paymentStartedAt),
            )
        }
        logProcessing(payment, success, reason)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()

private data class PendingPayment(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val transactionId: UUID,
)

private data class BulkPaymentRequest(
    val serviceName: String,
    val accountName: String,
    val requests: List<BulkPaymentItem>,
)

private data class BulkPaymentItem(
    val transactionId: String,
    val paymentId: String,
    val amount: Int,
)

private data class BulkPaymentResponse(
    val responses: List<ExternalSysResponse> = emptyList(),
)
