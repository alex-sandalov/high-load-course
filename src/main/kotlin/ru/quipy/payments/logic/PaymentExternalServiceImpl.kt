package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(1))
        .dispatcher(Dispatcher().apply {
            val permits = parallelRequests.coerceAtLeast(1)
            maxRequests = permits
            maxRequestsPerHost = permits
        })
        .build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1_020))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val reserve = requestAverageProcessingTime.toMillis().coerceAtLeast(1)
        var sent = false
        repeat(3) {
            if (now() + reserve > deadline) {
                if (!sent) close(paymentId, paymentStartedAt, false, "deadline exceeded before submit")
                return
            }
            val transactionId = UUID.randomUUID()
            ongoingWindow.acquire()
            try {
                val submittedAt = now()
                paymentESService.update(paymentId) {
                    it.logSubmission(true, transactionId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
                }
                sent = true
                val attempt = post(transactionId, paymentId, amount, deadline)
                paymentESService.update(paymentId) {
                    it.logProcessing(attempt.ok, now(), transactionId, attempt.reason)
                }
                if (attempt.ok || !attempt.retry) return
                if (attempt.wait > 0 && now() + attempt.wait + reserve > deadline) return
                if (attempt.wait > 0) Thread.sleep(attempt.wait)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } finally {
                ongoingWindow.release()
            }
        }
    }

    private fun post(transactionId: UUID, paymentId: UUID, amount: Int, deadline: Long): Attempt {
        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()
        while (!rateLimiter.tick()) {
            val pause = min(20L, deadline - now())
            if (pause <= 0) return Attempt(false, true, "rate limit left no time before deadline", 0)
            Thread.sleep(pause)
        }
        val remaining = Duration.ofMillis((deadline - now()).coerceAtLeast(1))
        val callClient = client.newBuilder().callTimeout(remaining).readTimeout(remaining).build()
        return try {
            callClient.newCall(request).execute().use { response ->
                val retryAfter = response.header("Retry-After")?.toLongOrNull()?.times(1_000)
                when {
                    response.code == 429 -> Attempt(false, true, "429 Too Many Requests", retryAfter ?: 1_000)
                    response.code in setOf(400, 401, 403, 404, 405) -> Attempt(false, false, "HTTP ${response.code}", 0)
                    response.code == 408 || response.code >= 500 -> Attempt(false, true, "HTTP ${response.code}", retryAfter ?: 200)
                    else -> readBody(response, transactionId, paymentId)
                }
            }
        } catch (e: Exception) {
            val timedOut = e is SocketTimeoutException || e is InterruptedIOException
            if (!timedOut) logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            Attempt(false, true, if (timedOut) "Request timeout." else e.message, if (timedOut) 0 else 200)
        }
    }

    private fun readBody(response: okhttp3.Response, transactionId: UUID, paymentId: UUID): Attempt {
        val raw = response.body?.string()
        val body = try {
            mapper.readValue(raw, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $raw")
            return Attempt(false, response.code >= 500, e.message, 200)
        }
        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
        return if (body.result) Attempt(true, false, body.message, 0) else Attempt(false, true, body.message, 0)
    }

    private fun close(paymentId: UUID, paymentStartedAt: Long, submitted: Boolean, reason: String) {
        val transactionId = UUID.randomUUID()
        val at = now()
        paymentESService.update(paymentId) {
            it.logSubmission(submitted, transactionId, at, Duration.ofMillis(at - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()

private data class Attempt(val ok: Boolean, val retry: Boolean, val reason: String?, val wait: Long)
