import com.example.data.GenuineMarketDataValidator
import com.example.data.GenuineMarketQuote
import com.example.data.ForexHistoricalDataProvider
import com.example.data.MarketAsset
import com.example.data.MarketCategory
import com.example.data.MarketConnectionState
import com.example.data.MarketFetchResult
import com.example.data.MarketUniverse
import com.example.data.PriceCandlestick
import com.example.data.PriceDataSource
import com.example.data.PriceHistoryResult
import com.example.data.RealMarketDataApiClient
import com.example.data.RealMarketDataEngine
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import okhttp3.Call
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import java.io.IOException

@RunWith(RobolectricTestRunner::class)
@OptIn(ExperimentalCoroutinesApi::class)
class RealMarketDataEngineTest {

    @Test
    fun testValidator_validatesTimestampsAndRejectsStaleData() {
        val nowSec = 1750000000L

        // Fresh crypto timestamp (60 seconds old)
        assertTrue(GenuineMarketDataValidator.isTimestampValid("BTC/USD", nowSec - 60, nowSec))

        // Stale crypto timestamp (700 seconds old > 600s threshold)
        assertFalse(GenuineMarketDataValidator.isTimestampValid("BTC/USD", nowSec - 700, nowSec))

        // Zero or negative timestamp rejected
        assertFalse(GenuineMarketDataValidator.isTimestampValid("EUR/USD", 0L, nowSec))
        assertFalse(GenuineMarketDataValidator.isTimestampValid("EUR/USD", -100L, nowSec))

        // Future timestamp with > 300s clock drift rejected
        assertFalse(GenuineMarketDataValidator.isTimestampValid("EUR/USD", nowSec + 1000, nowSec))

        // Normal market timestamp (2 hours old) accepted for forex/stocks
        assertTrue(GenuineMarketDataValidator.isTimestampValid("EUR/USD", nowSec - 7200, nowSec))

        // Stale market timestamp (> 48 hours) rejected
        assertFalse(GenuineMarketDataValidator.isTimestampValid("EUR/USD", nowSec - 200000, nowSec))
    }

    @Test
    fun testValidator_validatesPrices() {
        assertTrue(GenuineMarketDataValidator.isPriceValid(1.0850))
        assertTrue(GenuineMarketDataValidator.isPriceValid(83000.0))
        assertFalse(GenuineMarketDataValidator.isPriceValid(0.0))
        assertFalse(GenuineMarketDataValidator.isPriceValid(-10.5))
        assertFalse(GenuineMarketDataValidator.isPriceValid(Double.NaN))
        assertFalse(GenuineMarketDataValidator.isPriceValid(Double.POSITIVE_INFINITY))
    }

    @Test
    fun testEngine_neverGeneratesFakePricesWhenApiFails() = runTest {
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val testScope = TestScope(testDispatcher)

        // Mock client that always throws network exception
        val failingHttpClient = createMockHttpClient { req ->
            throw IOException("Simulated network outage (DNS resolution failed)")
        }
        val failingApiClient = RealMarketDataApiClient(failingHttpClient)
        val engine = RealMarketDataEngine(scope = testScope, apiClient = failingApiClient)

        // Run fetch cycle
        engine.fetchRealMarketDataOnce()

        // Verify connection state reports error and NEVER generates fake quotes
        val connState = engine.connectionState.value
        assertTrue("Expected error state on API failure, was $connState", connState is MarketConnectionState.Error)
        assertEquals(false, engine.isLoading.value)
        assertNotNull(engine.lastErrorMessage.value)

        // Quotes map must be empty — no random-walk prices were fabricated!
        assertTrue(engine.marketQuotes.value.isEmpty())

        // getQuoteForSymbol returns safe placeholder with lastPrice = 0.0 and timestamp = 0
        val quote = engine.getQuoteForSymbol("BTC/USD")
        assertEquals(0.0, quote.lastPrice, 0.0001)
        assertEquals(0L, quote.lastUpdatedTimestamp)
        assertEquals("Awaiting Live API", quote.volume24h)

        // Telemetry warning generated for AI
        val telemetry = engine.getTelemetrySummaryForAi(quote)
        assertTrue(telemetry.contains("STATUS: Live genuine API data unavailable"))
        assertTrue(telemetry.contains("NEVER execute trades or create fake signals"))
    }

    @Test
    fun testEngine_successfullyIngestsGenuineApiResponse() = runTest {
        val testDispatcher = StandardTestDispatcher(testScheduler)
        val testScope = TestScope(testDispatcher)

        val currentTimeSec = System.currentTimeMillis() / 1000

        val mockHttpClient = createMockHttpClient { req ->
            val url = req.url.toString()
            when {
                url.contains("binance.com/api/v3/ticker/24hr?symbol=BTCUSDT") -> {
                    createJsonResponse(
                        req,
                        """
                        {
                            "symbol": "BTCUSDT",
                            "lastPrice": "83150.25",
                            "bidPrice": "83150.20",
                            "askPrice": "83150.30",
                            "priceChangePercent": "2.45",
                            "highPrice": "84200.00",
                            "lowPrice": "81200.00",
                            "quoteVolume": "1850000000.00",
                            "closeTime": ${currentTimeSec * 1000}
                        }
                        """.trimIndent()
                    )
                }
                url.contains("binance.com/api/v3/klines") -> {
                    val klinesList = (1..60).map { i ->
                        val ts = 1700000000000L + (i * 3600000L)
                        val p = 82000.0 + (i * 15.0)
                        "[$ts,\"$p\",\"${p + 200}\",\"${p - 200}\",\"${p + 50}\",\"100\",${ts + 3600000L},\"8240000\",50,\"50\",\"4120000\",\"0\"]"
                    }
                    val klinesJson = "[" + klinesList.joinToString(",") + "]"
                    createJsonResponse(req, klinesJson)
                }
                url.contains("open.er-api.com/v6/latest/USD") -> {
                    createJsonResponse(
                        req,
                        """
                        {
                            "result": "success",
                            "time_last_update_unix": $currentTimeSec,
                            "rates": {
                                "EUR": 0.8928,
                                "GBP": 0.7570,
                                "JPY": 158.25
                            }
                        }
                        """.trimIndent()
                    )
                }
                url.contains("finance.yahoo.com/v8/finance/chart") -> {
                    val closesList = (1..60).map { 220.0 + it * 0.2 }
                    val timestampsList = (1..60).map { currentTimeSec - (60 - it) * 3600 }
                    val closesJson = closesList.joinToString(",")
                    val timestampsJson = timestampsList.joinToString(",")
                    createJsonResponse(
                        req,
                        """
                        {
                            "chart": {
                                "result": [
                                    {
                                        "meta": {
                                            "regularMarketPrice": 230.50,
                                            "chartPreviousClose": 228.00,
                                            "regularMarketDayHigh": 232.00,
                                            "regularMarketDayLow": 227.50,
                                            "regularMarketVolume": 85000000,
                                            "regularMarketTime": $currentTimeSec
                                        },
                                        "timestamp": [$timestampsJson],
                                        "indicators": {
                                            "quote": [
                                                { 
                                                    "open": [$closesJson],
                                                    "high": [$closesJson],
                                                    "low": [$closesJson],
                                                    "close": [$closesJson] 
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                        """.trimIndent()
                    )
                }
                else -> {
                    createJsonResponse(
                        req,
                        """
                        {
                            "symbol": "PAXGUSDT",
                            "lastPrice": "4180.50",
                            "bidPrice": "4180.40",
                            "askPrice": "4180.60",
                            "priceChangePercent": "0.85",
                            "highPrice": "4210.00",
                            "lowPrice": "4150.00",
                            "quoteVolume": "25000000.00",
                            "closeTime": ${currentTimeSec * 1000}
                        }
                        """.trimIndent()
                    )
                }
            }
        }

        // Create a mock forex provider that returns valid test candles
        val mockForexProvider = object : ForexHistoricalDataProvider() {
            override suspend fun fetchForexHistory(
                symbol: String,
                timeframe: String,
                limit: Int
            ): PriceHistoryResult {
                val now = System.currentTimeMillis()
                val candles = (0 until 60).map { i ->
                    val ts = now - (60 - i) * 3600000L
                    val price = 1.1200 + i * 0.0001
                    PriceCandlestick(ts, price, price + 0.0005, price - 0.0005, price)
                }
                return PriceHistoryResult.Success(symbol, timeframe, candles, PriceDataSource.TEST_DATA, now)
            }
        }

        val apiClient = RealMarketDataApiClient(mockHttpClient, mockForexProvider)
        val engine = RealMarketDataEngine(scope = testScope, apiClient = apiClient)

        engine.fetchRealMarketDataOnce()

        val connState = engine.connectionState.value
        assertTrue("Expected Connected state, got $connState", connState is MarketConnectionState.Connected)

        val btcQuote = engine.getQuoteForSymbol("BTC/USD")
        assertEquals(83150.25, btcQuote.lastPrice, 0.01)
        assertEquals(83150.20, btcQuote.bid, 0.01)
        assertEquals(83150.30, btcQuote.ask, 0.01)
        assertEquals(2.45, btcQuote.change24hPercent, 0.01)
        assertTrue(btcQuote.volume24h.contains("$1.85B"))
        assertTrue(btcQuote.lastUpdatedTimestamp > 0L)

        val eurQuote = engine.getQuoteForSymbol("EUR/USD")
        assertEquals(1.1259, eurQuote.lastPrice, 0.0001)

        val nvdaQuote = engine.getQuoteForSymbol("NVDA")
        assertEquals(230.50, nvdaQuote.lastPrice, 0.01)
    }

    private fun createJsonResponse(request: Request, bodyString: String): Response {
        val mediaType = "application/json; charset=utf-8".toMediaTypeOrNull()
        val responseBody = bodyString.toResponseBody(mediaType)
        return Response.Builder()
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .body(responseBody)
            .build()
    }

    private fun createMockHttpClient(handler: (Request) -> Response): OkHttpClient {
        return object : OkHttpClient() {
            override fun newCall(request: Request): Call {
                return object : Call {
                    override fun request(): Request = request
                    override fun execute(): Response = handler(request)
                    override fun enqueue(responseCallback: okhttp3.Callback) {
                        try {
                            responseCallback.onResponse(this, handler(request))
                        } catch (e: IOException) {
                            responseCallback.onFailure(this, e)
                        }
                    }
                    override fun cancel() {}
                    override fun isExecuted(): Boolean = true
                    override fun isCanceled(): Boolean = false
                    override fun clone(): Call = this
                    override fun timeout(): okio.Timeout = okio.Timeout.NONE
                }
            }
        }
    }
}
