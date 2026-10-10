package com.example.data

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.math.abs

/**
 * Fetches genuine historical OHLC candles for forex pairs from real market data APIs.
 *
 * Supported APIs:
 * 1. Finnhub Forex Candles (hourly/daily real-time data, requires free API key)
 * 2. Alpha Vantage FX_DAILY (daily candles, no key required but rate-limited to 5/min)
 *
 * NEVER generates synthetic data. Validates all candles:
 * - Positive OHLC prices
 * - Proper ordering: high >= max(open, close), low <= min(open, close)
 * - Monotonically increasing timestamps
 * - Minimum 50 candles for technical analysis
 * - Timestamp freshness for selected timeframe
 *
 * Returns PriceHistoryResult.Failure if:
 * - API unavailable or rate-limited
 * - Data malformed or invalid
 * - Insufficient historical candles
 * - Candles too stale for timeframe
 */
open class ForexHistoricalDataProvider(
    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(10, TimeUnit.SECONDS)
        .build(),
    private val finnhubApiKey: String? = null
) {
    /**
     * Fetch genuine historical forex candles.
     *
     * @param symbol "EUR/USD", "GBP/USD", or "USD/JPY"
     * @param timeframe "1h" (hourly) or "1d" (daily)
     * @param limit Maximum candles to return (1-500)
     * @return PriceHistoryResult.Success with validated real candles, or Failure with reason
     */
    open suspend fun fetchForexHistory(
        symbol: String,
        timeframe: String = "1h",
        limit: Int = 120
    ): PriceHistoryResult = withContext(Dispatchers.IO) {
        try {
            // Validate input
            if (timeframe !in listOf("1h", "1d")) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Invalid timeframe",
                    details = "Only '1h' (hourly) and '1d' (daily) supported. Got: $timeframe",
                    retryable = false
                )
            }

            // Try Finnhub first (hourly data, more current)
            val finnhubKey = finnhubApiKey ?: System.getenv("FINNHUB_API_KEY")
            if (!finnhubKey.isNullOrBlank()) {
                val result = tryFinnhubForex(symbol, timeframe, limit)
                if (result is PriceHistoryResult.Success) {
                    return@withContext result
                }
            }

            // Fallback to Alpha Vantage (daily only, free)
            if (timeframe == "1d") {
                val result = tryAlphaVantageForex(symbol, limit)
                if (result is PriceHistoryResult.Success) {
                    return@withContext result
                }
            }

            // Both failed
            PriceHistoryResult.Failure(
                reason = "Forex historical data unavailable",
                details = "No genuine OHLC provider available. Configure FINNHUB_API_KEY for production.",
                retryable = true
            )
        } catch (e: Exception) {
            PriceHistoryResult.Failure(
                reason = "Forex provider exception",
                details = "${e.javaClass.simpleName}: ${e.message}",
                retryable = true
            )
        }
    }

    /**
     * Finnhub Forex Candles API
     * https://finnhub.io/docs/api#forex-candles
     *
     * Real-time hourly/daily candles (OHLCV) with genuine market data.
     * Requires free API key from https://finnhub.io
     */
    private suspend fun tryFinnhubForex(
        symbol: String,
        timeframe: String,
        limit: Int
    ): PriceHistoryResult = withContext(Dispatchers.IO) {
        try {
            val finnhubSymbol = normalizeForexSymbol(symbol)
            val resolution = when (timeframe) {
                "1h" -> "60"
                "1d" -> "D"
                else -> "60"
            }

            val nowSeconds = System.currentTimeMillis() / 1000L
            val fromSeconds = nowSeconds - (limit * 3600L)

            val key = System.getenv("FINNHUB_API_KEY") ?: return@withContext PriceHistoryResult.Failure(
                reason = "No Finnhub API key",
                details = "Set FINNHUB_API_KEY environment variable",
                retryable = false
            )

            val url = "https://finnhub.io/api/v1/forex/candle" +
                    "?symbol=$finnhubSymbol" +
                    "&resolution=$resolution" +
                    "&from=$fromSeconds" +
                    "&to=$nowSeconds" +
                    "&token=$key"

            val request = Request.Builder()
                .url(url)
                .header("User-Agent", "CashFlowAI/1.0")
                .build()

            val response = client.newCall(request).execute()
            if (!response.isSuccessful) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Finnhub HTTP ${response.code}",
                    details = "API request failed",
                    retryable = true
                )
            }

            val json = JSONObject(response.body?.string() ?: "{}")

            // Check for API errors
            if (json.has("error")) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Finnhub error",
                    details = json.getString("error"),
                    retryable = false
                )
            }

            // Parse OHLCV arrays
            val timestamps = json.optJSONArray("t") ?: JSONArray()
            val opens = json.optJSONArray("o") ?: JSONArray()
            val highs = json.optJSONArray("h") ?: JSONArray()
            val lows = json.optJSONArray("l") ?: JSONArray()
            val closes = json.optJSONArray("c") ?: JSONArray()
            val volumes = json.optJSONArray("v") ?: JSONArray()

            if (timestamps.length() == 0) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "No candles in response",
                    details = "Finnhub returned empty OHLC array",
                    retryable = true
                )
            }

            val candlesticks = mutableListOf<PriceCandlestick>()
            for (i in 0 until timestamps.length()) {
                try {
                    val ts = timestamps.getLong(i) * 1000L
                    val open = opens.getDouble(i)
                    val high = highs.getDouble(i)
                    val low = lows.getDouble(i)
                    val close = closes.getDouble(i)
                    val volume = if (i < volumes.length()) volumes.getDouble(i) else null

                    // Validate OHLC
                    if (!isValidOhlc(open, high, low, close)) continue

                    candlesticks.add(
                        PriceCandlestick(
                            timestamp = ts,
                            open = open,
                            high = high,
                            low = low,
                            close = close,
                            volume = volume
                        )
                    )
                } catch (e: Exception) {
                    continue
                }
            }

            // Validate complete series
            if (!validateCandleSeries(candlesticks, symbol, timeframe)) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Invalid candle series",
                    details = "Candlesticks failed validation (size: ${candlesticks.size}, ordering, freshness)",
                    retryable = false
                )
            }

            PriceHistoryResult.Success(
                symbol = symbol,
                timeframe = timeframe,
                candlesticks = candlesticks,
                source = PriceDataSource.MARKET_DATA_API,
                fetchedAt = System.currentTimeMillis()
            )
        } catch (e: Exception) {
            PriceHistoryResult.Failure(
                reason = "Finnhub parse error",
                details = e.message ?: e.toString(),
                retryable = false
            )
        }
    }

    /**
     * Alpha Vantage FX_DAILY
     * https://www.alphavantage.co/documentation/#fx-daily
     *
     * Daily forex candles. Free tier (demo key) heavily rate-limited.
     * Fallback only when Finnhub unavailable.
     */
    private suspend fun tryAlphaVantageForex(
        symbol: String,
        limit: Int
    ): PriceHistoryResult = withContext(Dispatchers.IO) {
        try {
            val parts = symbol.split("/")
            if (parts.size != 2) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Invalid symbol format",
                    details = "Expected 'XXX/YYY', got '$symbol'",
                    retryable = false
                )
            }

            val url = "https://www.alphavantage.co/query" +
                    "?function=FX_DAILY" +
                    "&from_symbol=${parts[0]}" +
                    "&to_symbol=${parts[1]}" +
                    "&outputsize=full" +
                    "&apikey=demo"  // Demo key: 5 calls/min

            val request = Request.Builder()
                .url(url)
                .header("User-Agent", "CashFlowAI/1.0")
                .build()

            val response = client.newCall(request).execute()
            if (!response.isSuccessful) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Alpha Vantage HTTP ${response.code}",
                    details = "",
                    retryable = true
                )
            }

            val json = JSONObject(response.body?.string() ?: "{}")

            // Check for rate limit or error
            if (json.has("Note")) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Alpha Vantage rate limit",
                    details = json.getString("Note"),
                    retryable = true
                )
            }
            if (json.has("Error Message")) {
                return@withContext PriceHistoryResult.Failure(
                    reason = json.getString("Error Message"),
                    details = "",
                    retryable = false
                )
            }

            val timeSeries = json.optJSONObject("Time Series FX (Daily)") ?: JSONObject()
            if (timeSeries.length() == 0) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "No time series data",
                    details = "Symbol not supported",
                    retryable = true
                )
            }

            val candlesticks = mutableListOf<PriceCandlestick>()
            val dateFormatter = java.text.SimpleDateFormat("yyyy-MM-dd", java.util.Locale.US)
            dateFormatter.timeZone = java.util.TimeZone.getTimeZone("UTC")

            val keys = timeSeries.keys().asSequence().toList().take(limit)
            for (dateStr in keys) {
                try {
                    val dayData = timeSeries.getJSONObject(dateStr)
                    val open = dayData.getDouble("1. open")
                    val high = dayData.getDouble("2. high")
                    val low = dayData.getDouble("3. low")
                    val close = dayData.getDouble("4. close")

                    if (!isValidOhlc(open, high, low, close)) continue

                    val date = dateFormatter.parse(dateStr) ?: continue
                    candlesticks.add(
                        PriceCandlestick(
                            timestamp = date.time,
                            open = open,
                            high = high,
                            low = low,
                            close = close,
                            volume = null
                        )
                    )
                } catch (e: Exception) {
                    continue
                }
            }

            candlesticks.sortBy { it.timestamp }

            // Validate
            if (!validateCandleSeries(candlesticks, symbol, "1d")) {
                return@withContext PriceHistoryResult.Failure(
                    reason = "Invalid candle series",
                    details = "Size: ${candlesticks.size}, failed validation",
                    retryable = false
                )
            }

            PriceHistoryResult.Success(
                symbol = symbol,
                timeframe = "1d",
                candlesticks = candlesticks,
                source = PriceDataSource.MARKET_DATA_API,
                fetchedAt = System.currentTimeMillis()
            )
        } catch (e: Exception) {
            PriceHistoryResult.Failure(
                reason = "Alpha Vantage parse error",
                details = e.message ?: e.toString(),
                retryable = false
            )
        }
    }

    private fun normalizeForexSymbol(symbol: String): String {
        return when (symbol) {
            "EUR/USD" -> "EURUSD"
            "GBP/USD" -> "GBPUSD"
            "USD/JPY" -> "USDJPY"
            else -> symbol.replace("/", "")
        }
    }

    /**
     * Validate single OHLC candle:
     * - All prices > 0
     * - High >= max(open, close)
     * - Low <= min(open, close)
     * - No NaN or Infinite
     */
    private fun isValidOhlc(open: Double, high: Double, low: Double, close: Double): Boolean {
        if (open <= 0.0 || high <= 0.0 || low <= 0.0 || close <= 0.0) return false
        if (open.isNaN() || high.isNaN() || low.isNaN() || close.isNaN()) return false
        if (open.isInfinite() || high.isInfinite() || low.isInfinite() || close.isInfinite()) return false
        if (high < maxOf(open, close)) return false
        if (low > minOf(open, close)) return false
        return true
    }

    /**
     * Validate complete candle series:
     * - Minimum 50 candles for technical analysis
     * - Timestamps strictly monotonically increasing
     * - Last candle not older than max staleness for timeframe
     */
    private fun validateCandleSeries(
        candles: List<PriceCandlestick>,
        symbol: String,
        timeframe: String
    ): Boolean {
        if (candles.size < 50) return false

        // Check monotonic increasing timestamps
        for (i in 1 until candles.size) {
            if (candles[i].timestamp <= candles[i - 1].timestamp) return false
        }

        // Check freshness
        val lastCandleAgeSeconds = (System.currentTimeMillis() - candles.last().timestamp) / 1000L
        val maxStalenessSeconds = when (timeframe) {
            "1h" -> 3600L  // Hourly: last candle should be < 1 hour old
            "1d" -> 86400L  // Daily: last candle should be < 1 day old
            else -> 172800L
        }

        // Allow some buffer for market holidays/weekends (forex closed)
        if (lastCandleAgeSeconds > maxStalenessSeconds * 3) return false

        return true
    }
}
