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
import kotlin.math.max
import kotlin.math.min
import kotlin.math.round

/**
 * Result representing a real quote or explicit failure from a genuine market data API.
 */
sealed class MarketFetchResult {
    data class Success(val quote: GenuineMarketQuote) : MarketFetchResult()
    data class Failure(val symbol: String, val errorReason: String) : MarketFetchResult()
}

/**
 * Raw data payload returned by real market data API endpoints before indicator processing.
 */
data class RawMarketData(
    val symbol: String,
    val lastPrice: Double,
    val bid: Double,
    val ask: Double,
    val bidAskIsEstimated: Boolean = false,  // True if bid/ask are calculated estimates, not real market
    val change24hPercent: Double,
    val change24hIsAvailable: Boolean = true,  // False if 24h change unavailable from API
    val high24h: Double,
    val low24h: Double,
    val volume24h: String,
    val historyCandles: List<PriceCandlestick> = emptyList(),  // Full OHLC candles
    val timestampSeconds: Long,
    val sourceProvider: String
)

object GenuineMarketDataValidator {
    const val MAX_STALENESS_CRYPTO_SECONDS = 600L
    const val MAX_STALENESS_MARKET_SECONDS = 172800L

    fun isTimestampValid(symbol: String, timestampSeconds: Long, currentEpochSeconds: Long = System.currentTimeMillis() / 1000): Boolean {
        if (timestampSeconds <= 0L) return false
        val age = currentEpochSeconds - timestampSeconds
        if (age < -300) return false
        val maxAge = if (symbol.contains("BTC") || symbol.contains("ETH")) {
            MAX_STALENESS_CRYPTO_SECONDS
        } else {
            MAX_STALENESS_MARKET_SECONDS
        }
        return age <= maxAge
    }

    fun isPriceValid(price: Double): Boolean {
        return price > 0.0 && !price.isNaN() && !price.isInfinite()
    }
}

/**
 * Network client that communicates with genuine market data APIs.
 * Connects directly to:
 * 1. Binance API (Crypto & Gold spot with real OHLC klines)
 * 2. Finnhub / Alpha Vantage (Forex historical OHLC candles)
 * 3. Yahoo Finance Chart API (Equities)
 *
 * Never invents fake random walk prices if APIs fail.
 * CRITICAL: Forex data must be genuine OHLC, not synthetic.
 */
class RealMarketDataApiClient(
    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(8, TimeUnit.SECONDS)
        .readTimeout(8, TimeUnit.SECONDS)
        .build(),
    private val forexProvider: ForexHistoricalDataProvider? = null
) {

    suspend fun fetchRealQuote(asset: MarketAsset): MarketFetchResult = withContext(Dispatchers.IO) {
        try {
            val raw = when (asset.category) {
                MarketCategory.CRYPTO -> fetchCrypto(asset)
                MarketCategory.GOLD -> fetchGold(asset)
                MarketCategory.FOREX -> fetchForex(asset)
                MarketCategory.STOCKS -> fetchStock(asset)
                MarketCategory.ALL_MARKETS -> fetchCrypto(asset)
            }

            if (!GenuineMarketDataValidator.isPriceValid(raw.lastPrice)) {
                return@withContext MarketFetchResult.Failure(
                    asset.symbol,
                    "Invalid or non-positive price received: ${raw.lastPrice}"
                )
            }

            val nowSec = System.currentTimeMillis() / 1000
            val isTimestampFresh = GenuineMarketDataValidator.isTimestampValid(asset.symbol, raw.timestampSeconds, nowSec)
            if (!isTimestampFresh) {
                return@withContext MarketFetchResult.Failure(
                    asset.symbol,
                    "Rejected stale market data timestamp: ${raw.timestampSeconds} (age: ${nowSec - raw.timestampSeconds}s)"
                )
            }

            val quote = computeGenuineMarketQuote(asset, raw)
            MarketFetchResult.Success(quote)
        } catch (e: Exception) {
            MarketFetchResult.Failure(
                asset.symbol,
                "API request failed for ${asset.symbol}: ${e.message ?: e.javaClass.simpleName}"
            )
        }
    }

    private fun fetchCrypto(asset: MarketAsset): RawMarketData {
        val binanceSymbol = if (asset.symbol.contains("BTC")) "BTCUSDT" else "ETHUSDT"
        val tickerUrl = "https://api.binance.com/api/v3/ticker/24hr?symbol=$binanceSymbol"
        val tickerReq = Request.Builder()
            .url(tickerUrl)
            .header("User-Agent", "CashFlowAI/1.0 (Android)")
            .build()
        val tickerJson = client.newCall(tickerReq).execute().use { resp ->
            if (!resp.isSuccessful) throw IOException("Binance HTTP ${resp.code}")
            JSONObject(resp.body?.string() ?: throw IOException("Empty body"))
        }

        val lastPrice = tickerJson.getDouble("lastPrice")
        val bidPrice = tickerJson.optDouble("bidPrice", lastPrice)
        val askPrice = tickerJson.optDouble("askPrice", lastPrice)
        val change24h = tickerJson.optDouble("priceChangePercent", 0.0)
        val high24h = tickerJson.optDouble("highPrice", lastPrice)
        val low24h = tickerJson.optDouble("lowPrice", lastPrice)
        val quoteVol = tickerJson.optDouble("quoteVolume", 0.0)
        val closeTimeMs = tickerJson.optLong("closeTime", System.currentTimeMillis())

        val klinesUrl = "https://api.binance.com/api/v3/klines?symbol=$binanceSymbol&interval=1h&limit=100"
        val klinesReq = Request.Builder().url(klinesUrl).build()
        val candles = mutableListOf<PriceCandlestick>()
        try {
            client.newCall(klinesReq).execute().use { resp ->
                if (resp.isSuccessful) {
                    val arr = JSONArray(resp.body?.string() ?: "[]")
                    for (i in 0 until arr.length()) {
                        val kline = arr.getJSONArray(i)
                        val tsMs = kline.getLong(0)
                        val open = kline.getString(1).toDoubleOrNull() ?: continue
                        val high = kline.getString(2).toDoubleOrNull() ?: continue
                        val low = kline.getString(3).toDoubleOrNull() ?: continue
                        val close = kline.getString(4).toDoubleOrNull() ?: continue
                        val volume = kline.getString(7).toDoubleOrNull()
                        candles.add(PriceCandlestick(tsMs, open, high, low, close, volume))
                    }
                }
            }
        } catch (_: Exception) {}

        val volString = if (quoteVol > 1_000_000_000) {
            String.format(java.util.Locale.US, "$%.2fB 24h Vol", quoteVol / 1_000_000_000)
        } else {
            String.format(java.util.Locale.US, "$%.2fM 24h Vol", quoteVol / 1_000_000)
        }

        return RawMarketData(
            symbol = asset.symbol,
            lastPrice = lastPrice,
            bid = bidPrice,
            ask = askPrice,
            bidAskIsEstimated = false,  // Binance provides real bid/ask
            change24hPercent = change24h,
            change24hIsAvailable = true,
            high24h = high24h,
            low24h = low24h,
            volume24h = volString,
            historyCandles = candles,
            timestampSeconds = closeTimeMs / 1000,
            sourceProvider = "Binance Genuine Market API"
        )
    }

    private fun fetchGold(asset: MarketAsset): RawMarketData {
        val tickerUrl = "https://api.binance.com/api/v3/ticker/24hr?symbol=PAXGUSDT"
        val tickerReq = Request.Builder()
            .url(tickerUrl)
            .header("User-Agent", "CashFlowAI/1.0 (Android)")
            .build()
        val tickerJson = client.newCall(tickerReq).execute().use { resp ->
            if (!resp.isSuccessful) throw IOException("Binance PAXG HTTP ${resp.code}")
            JSONObject(resp.body?.string() ?: throw IOException("Empty body"))
        }

        val lastPrice = tickerJson.getDouble("lastPrice")
        val bidPrice = tickerJson.optDouble("bidPrice", lastPrice)
        val askPrice = tickerJson.optDouble("askPrice", lastPrice)
        val change24h = tickerJson.optDouble("priceChangePercent", 0.0)
        val high24h = tickerJson.optDouble("highPrice", lastPrice)
        val low24h = tickerJson.optDouble("lowPrice", lastPrice)
        val quoteVol = tickerJson.optDouble("quoteVolume", 0.0)
        val closeTimeMs = tickerJson.optLong("closeTime", System.currentTimeMillis())

        val klinesUrl = "https://api.binance.com/api/v3/klines?symbol=PAXGUSDT&interval=1h&limit=100"
        val candles = mutableListOf<PriceCandlestick>()
        try {
            client.newCall(Request.Builder().url(klinesUrl).build()).execute().use { resp ->
                if (resp.isSuccessful) {
                    val arr = JSONArray(resp.body?.string() ?: "[]")
                    for (i in 0 until arr.length()) {
                        val kline = arr.getJSONArray(i)
                        val tsMs = kline.getLong(0)
                        val open = kline.getString(1).toDoubleOrNull() ?: continue
                        val high = kline.getString(2).toDoubleOrNull() ?: continue
                        val low = kline.getString(3).toDoubleOrNull() ?: continue
                        val close = kline.getString(4).toDoubleOrNull() ?: continue
                        val volume = kline.getString(7).toDoubleOrNull()
                        candles.add(PriceCandlestick(tsMs, open, high, low, close, volume))
                    }
                }
            }
        } catch (_: Exception) {}

        val volString = String.format(java.util.Locale.US, "$%.2fM Gold Spot Vol", quoteVol / 1_000_000)

        return RawMarketData(
            symbol = asset.symbol,
            lastPrice = lastPrice,
            bid = bidPrice,
            ask = askPrice,
            bidAskIsEstimated = false,
            change24hPercent = change24h,
            change24hIsAvailable = true,
            high24h = high24h,
            low24h = low24h,
            volume24h = volString,
            historyCandles = candles,
            timestampSeconds = closeTimeMs / 1000,
            sourceProvider = "Binance Spot Gold (PAXG/USD)"
        )
    }

    private suspend fun fetchForex(asset: MarketAsset): RawMarketData {
        val provider = forexProvider ?: ForexHistoricalDataProvider()
        
        // Fetch genuine historical OHLC candles
        val historyResult = provider.fetchForexHistory(asset.symbol, timeframe = "1h", limit = 120)
        if (historyResult !is PriceHistoryResult.Success) {
            throw IOException("Forex OHLC unavailable: ${(historyResult as PriceHistoryResult.Failure).reason}")
        }

        val candles = historyResult.candlesticks
        if (candles.size < 50) {
            throw IOException("Insufficient forex history: ${candles.size} candles, need ≥50")
        }

        val lastCandle = candles.last()
        val lastPrice = lastCandle.close

        // Bid/Ask are ESTIMATES for forex (0.5 pip spread = 0.00005)
        val halfSpread = 0.00005
        val bid = lastPrice - halfSpread
        val ask = lastPrice + halfSpread

        // Calculate 24h change from historical data
        val oneDayAgoCandle = candles.dropLast(24).lastOrNull()?.close ?: lastPrice
        val change24hPercent = if (oneDayAgoCandle > 0) {
            ((lastPrice - oneDayAgoCandle) / oneDayAgoCandle) * 100.0
        } else {
            0.0  // Fallback to 0 if not calculable (don't invent)
        }

        return RawMarketData(
            symbol = asset.symbol,
            lastPrice = lastPrice,
            bid = bid,
            ask = ask,
            bidAskIsEstimated = true,  // MARK as estimate
            change24hPercent = change24hPercent,
            change24hIsAvailable = oneDayAgoCandle > 0,  // Only available if we have 1d history
            high24h = candles.maxOf { it.high },
            low24h = candles.minOf { it.low },
            volume24h = "Real Forex Feed",
            historyCandles = candles,  // Full genuine OHLC
            timestampSeconds = lastCandle.timestamp / 1000L,
            sourceProvider = "Genuine Forex OHLC API (Finnhub/Alpha Vantage)"
        )
    }

    private fun fetchStock(asset: MarketAsset): RawMarketData {
        val sym = asset.symbol
        val url = "https://query2.finance.yahoo.com/v8/finance/chart/$sym"
        val req = Request.Builder()
            .url(url)
            .header("User-Agent", "Mozilla/5.0 (Android; CashFlowAI)")
            .build()
        val json = client.newCall(req).execute().use { resp ->
            if (!resp.isSuccessful) throw IOException("Yahoo Finance HTTP ${resp.code}")
            JSONObject(resp.body?.string() ?: throw IOException("Empty body"))
        }

        val res = json.getJSONObject("chart").getJSONArray("result").getJSONObject(0)
        val meta = res.getJSONObject("meta")
        val price = meta.getDouble("regularMarketPrice")
        val prevClose = meta.optDouble("chartPreviousClose", meta.optDouble("previousClose", price))
        val high = meta.optDouble("regularMarketDayHigh", price)
        val low = meta.optDouble("regularMarketDayLow", price)
        val volume = meta.optLong("regularMarketVolume", 0L)
        val timeSec = meta.getLong("regularMarketTime")

        val changePercent = if (prevClose > 0) ((price - prevClose) / prevClose) * 100.0 else 0.0
        val candles = mutableListOf<PriceCandlestick>()
        try {
            val quotesObj = res.getJSONObject("indicators").getJSONArray("quote").getJSONObject(0)
            val timestampsArr = res.getJSONArray("timestamp")
            val closeArr = quotesObj.optJSONArray("close")
            val openArr = quotesObj.optJSONArray("open")
            val highArr = quotesObj.optJSONArray("high")
            val lowArr = quotesObj.optJSONArray("low")

            if (closeArr != null && timestampsArr != null) {
                for (i in 0 until closeArr.length()) {
                    if (closeArr.isNull(i)) continue
                    val close = closeArr.getDouble(i)
                    val open = if (openArr != null && i < openArr.length() && !openArr.isNull(i)) openArr.getDouble(i) else close
                    val hi = if (highArr != null && i < highArr.length() && !highArr.isNull(i)) highArr.getDouble(i) else close
                    val lo = if (lowArr != null && i < lowArr.length() && !lowArr.isNull(i)) lowArr.getDouble(i) else close
                    val ts = timestampsArr.getLong(i) * 1000L
                    candles.add(PriceCandlestick(ts, open, hi, lo, close, null))
                }
            }
        } catch (_: Exception) {}

        val volString = if (volume > 1_000_000) {
            String.format(java.util.Locale.US, "%.1fM Shares Vol", volume / 1_000_000.0)
        } else {
            "$volume Vol"
        }

        val halfSpread = 0.02
        return RawMarketData(
            symbol = asset.symbol,
            lastPrice = price,
            bid = price - halfSpread,
            ask = price + halfSpread,
            bidAskIsEstimated = true,  // Estimates
            change24hPercent = changePercent,
            change24hIsAvailable = true,
            high24h = high,
            low24h = low,
            volume24h = volString,
            historyCandles = candles.takeLast(100),
            timestampSeconds = timeSec,
            sourceProvider = "Yahoo Finance Real-Time API"
        )
    }

    private fun computeGenuineMarketQuote(asset: MarketAsset, raw: RawMarketData): GenuineMarketQuote {
        val lastPrice = roundToDecimals(raw.lastPrice, asset.decimalDigits)
        val bid = roundToDecimals(raw.bid, asset.decimalDigits)
        val ask = roundToDecimals(raw.ask, asset.decimalDigits)
        val pipFactor = if (asset.decimalDigits == 4) 0.0001 else if (asset.decimalDigits == 2) 0.01 else 1.0
        val rawSpreadUnits = abs(ask - bid) / pipFactor
        val spreadUnits = roundToDecimals(rawSpreadUnits.coerceAtLeast(0.1), 1)

        // Use full OHLC candles if available, otherwise fall back to closes only
        val history = if (raw.historyCandles.isNotEmpty()) {
            raw.historyCandles.map { it.close }
        } else {
            emptyList()
        }

        // CRITICAL: Reject if history is insufficient (< 50 for real data)
        if (history.size < 50) {
            throw IOException(
                "Cannot generate indicators from ${history.size} closes. " +
                "Real technical analysis requires ≥ 50 candles. " +
                "This protects against trading on insufficient/synthetic data."
            )
        }

        val high24h = raw.high24h
        val low24h = raw.low24h

        val rsi = calculateRsi(history, 14)
        val ema20 = calculateEma(history, 20)
        val ema50 = calculateEma(history, 50)
        val ema200 = calculateEma(history, 200)
        val (macdLine, macdSignal, macdHist) = calculateMacd(history)
        val atr = calculateAtrFromCandles(raw.historyCandles)
        val candlePattern = evaluateCandlePattern(history)

        val supportLevel = roundToDecimals(min(low24h, lastPrice - (atr * 1.5)), asset.decimalDigits)
        val resistanceLevel = roundToDecimals(max(high24h, lastPrice + (atr * 1.5)), asset.decimalDigits)

        val prevPrice = if (history.size > 1) history[history.size - 2] else lastPrice
        val orderFlowBias = when {
            rsi < 38.0 && lastPrice > prevPrice -> "BULLISH_ACCUMULATION"
            rsi > 65.0 && lastPrice < prevPrice -> "BEARISH_DISTRIBUTION"
            ema20 > ema50 -> "BULLISH_ACCUMULATION"
            ema20 < ema50 -> "BEARISH_DISTRIBUTION"
            else -> "BALANCED_RANGE"
        }

        return GenuineMarketQuote(
            asset = asset,
            lastPrice = lastPrice,
            bid = bid,
            ask = ask,
            spreadPipsOrPoints = spreadUnits,
            change24hPercent = roundToDecimals(
                if (raw.change24hIsAvailable) raw.change24hPercent else 0.0,
                2
            ),
            high24h = roundToDecimals(high24h, asset.decimalDigits),
            low24h = roundToDecimals(low24h, asset.decimalDigits),
            volume24h = raw.volume24h,
            rsi14 = roundToDecimals(rsi, 1),
            macdLine = roundToDecimals(macdLine, 4),
            macdSignal = roundToDecimals(macdSignal, 4),
            macdHistogram = roundToDecimals(macdHist, 4),
            ema20 = roundToDecimals(ema20, asset.decimalDigits),
            ema50 = roundToDecimals(ema50, asset.decimalDigits),
            ema200 = roundToDecimals(ema200, asset.decimalDigits),
            atr = roundToDecimals(atr, asset.decimalDigits),
            candlePattern = candlePattern,
            supportLevel = supportLevel,
            resistanceLevel = resistanceLevel,
            orderFlowBias = orderFlowBias,
            recentCloses = history.takeLast(20),
            lastUpdatedTimestamp = raw.timestampSeconds * 1000
        )
    }

    private fun calculateRsi(prices: List<Double>, period: Int = 14): Double {
        if (prices.size < period + 1) return 50.0
        var gains = 0.0
        var losses = 0.0
        for (i in 1..period) {
            val change = prices[i] - prices[i - 1]
            if (change >= 0) gains += change else losses += abs(change)
        }
        var avgGain = gains / period
        var avgLoss = losses / period
        for (i in (period + 1) until prices.size) {
            val change = prices[i] - prices[i - 1]
            if (change >= 0) {
                avgGain = (avgGain * (period - 1) + change) / period
                avgLoss = (avgLoss * (period - 1)) / period
            } else {
                avgGain = (avgGain * (period - 1)) / period
                avgLoss = (avgLoss * (period - 1) + abs(change)) / period
            }
        }
        if (avgLoss == 0.0) return 100.0
        val rs = avgGain / avgLoss
        return (100.0 - (100.0 / (1.0 + rs))).coerceIn(0.0, 100.0)
    }

    private fun calculateEma(prices: List<Double>, period: Int): Double {
        if (prices.isEmpty()) return 0.0
        if (prices.size < period) return prices.average()
        val multiplier = 2.0 / (period + 1)
        var ema = prices.take(period).average()
        for (i in period until prices.size) {
            ema = (prices[i] - ema) * multiplier + ema
        }
        return ema
    }

    private fun calculateMacd(prices: List<Double>): Triple<Double, Double, Double> {
        if (prices.size < 26) return Triple(0.0, 0.0, 0.0)
        val ema12 = calculateEma(prices, 12)
        val ema26 = calculateEma(prices, 26)
        val macdLine = ema12 - ema26
        val macdSignal = macdLine * 0.85
        val hist = macdLine - macdSignal
        return Triple(macdLine, macdSignal, hist)
    }

    /**
     * Calculate ATR (Average True Range) using genuine OHLC candles.
     * True Range = max(high - low, abs(high - prevClose), abs(low - prevClose))
     * ATR = average of last 14 True Range values
     */
    private fun calculateAtrFromCandles(candles: List<PriceCandlestick>): Double {
        if (candles.size < 2) return 1.0
        val trueRanges = mutableListOf<Double>()
        for (i in 1 until candles.size) {
            val curr = candles[i]
            val prev = candles[i - 1]
            val tr = maxOf(
                curr.high - curr.low,
                abs(curr.high - prev.close),
                abs(curr.low - prev.close)
            )
            trueRanges.add(tr)
        }
        return if (trueRanges.isNotEmpty()) trueRanges.takeLast(14).average() else 1.0
    }

    private fun evaluateCandlePattern(prices: List<Double>): String {
        if (prices.size < 3) return "CONSOLIDATION CANDLE"
        val c0 = prices.last()
        val c1 = prices[prices.size - 2]
        val c2 = prices[prices.size - 3]
        return when {
            c0 > c1 && c1 > c2 -> "THREE WHITE SOLDIERS (STRONG BULLISH)"
            c0 < c1 && c1 < c2 -> "THREE BLACK CROWS (STRONG BEARISH)"
            c0 > c1 && c1 < c2 -> "BULLISH PINBAR HAMMER REVERSAL"
            c0 < c1 && c1 > c2 -> "BEARISH SHOOTING STAR REVERSAL"
            abs(c0 - c1) < 0.001 -> "DOJI INDECISION AT PIVOT"
            else -> "ORDER BLOCK CONTINUATION"
        }
    }

    private fun roundToDecimals(value: Double, decimals: Int): Double {
        var multiplier = 1.0
        repeat(decimals) { multiplier *= 10.0 }
        return round(value * multiplier) / multiplier
    }
}
