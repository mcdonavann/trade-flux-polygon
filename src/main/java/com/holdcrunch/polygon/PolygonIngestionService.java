package com.holdcrunch.polygon;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PolygonIngestionService extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(PolygonIngestionService.class);

    private static final int BATCH = 200;
    private static final int WS_SUB_BATCH = 250;

    private static final long WS_HEARTBEAT_MS = 10_000;
    private static final long WS_RECONNECT_DELAY = 5_000;
    private static final long TRADES_LOG_INTERVAL_MS = 10 * 60 * 1000;
    private static final long FLUSH_INTERVAL_MS = 250;

    private static final int MAX_PENDING_QUEUE = 50_000;

    private final Config cfg;
    private final WebClient webClient;
    private final PgPool pool;

    private HttpClient httpClient;
    private WebSocket ws;

    private Long pingTimerId;
    private Long tradesLogTimerId;
    private Long flushTimerId;
    private Long reconnectTimerId;
    private Long backfillTimerId;

    private final AtomicLong lastInserted = new AtomicLong(0);
    private final AtomicLong lastLoggedInserted = new AtomicLong(0);
    private final AtomicLong droppedTrades = new AtomicLong(0);
    private final AtomicLong lastLoggedDropped = new AtomicLong(0);

    private final AtomicBoolean flushInFlight = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean backfillInFlight = new AtomicBoolean(false);

    private final Queue<Tuple> pendingTrades = new ArrayDeque<>();

    private static final String JOB_NAME = "polygon_trades";
    private static final String GET_LAST_TIMESTAMP = "SELECT last_processed_block FROM sync_state WHERE job_name = $1";
    private static final String UPSERT_SYNC_STATE = """
        INSERT INTO sync_state (job_name, last_processed_block, last_success_at)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (job_name) DO UPDATE SET last_processed_block = EXCLUDED.last_processed_block, last_success_at = CURRENT_TIMESTAMP
        """;
    private static final int SUBGRAPH_RETRIES = 3;
    private static final long SUBGRAPH_RETRY_DELAY_MS = 2000;

    private static final String INSERT_TRADE = """
        INSERT INTO polygon_trades (
            order_hash,
            block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            maker,
            taker,
            maker_asset_id,
            taker_asset_id,
            maker_amount,
            taker_amount,
            fee,
            condition_id,
            token_id,
            price,
            shares,
            side
        )
        VALUES ($1,$2,to_timestamp($3)::timestamptz,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        ON CONFLICT (order_hash) DO NOTHING
        """;

    public PolygonIngestionService(Config cfg, WebClient webClient, PgPool pool) {
        this.cfg = cfg;
        this.webClient = webClient;
        this.pool = pool;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        log.info("Polygon ingestion starting");

        httpClient = vertx.createHttpClient();

        flushTimerId = vertx.setPeriodic(FLUSH_INTERVAL_MS, id -> flushPendingTrades());
        tradesLogTimerId = vertx.setPeriodic(TRADES_LOG_INTERVAL_MS, id -> logTradesInserted());

        // Optional initial backfill hook
        startInitialBackfill();

        if (cfg.polygonWsLiveEnabled) {
            connectWebSocket();
        } else {
            log.info("Polygon WS live ingestion disabled by config");
        }

        startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        stopping.set(true);

        cancelTimer(pingTimerId);
        cancelTimer(tradesLogTimerId);
        cancelTimer(flushTimerId);
        cancelTimer(reconnectTimerId);
        cancelTimer(backfillTimerId);

        try {
            if (ws != null && !ws.isClosed()) {
                ws.close();
            }
        } catch (Exception ignored) {
        }

        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (Exception ignored) {
        }

        flushPendingTrades();
        stopPromise.complete();
    }

    private void startInitialBackfill() {
        if (!cfg.polygonSubgraphBackfillEnabled) {
            log.info("Subgraph backfill disabled by config");
            return;
        }
        runSubgraphBackfill("initial")
            .onSuccess(v -> {
                backfillTimerId = vertx.setPeriodic(cfg.polygonSubgraphBackfillIntervalMs, id ->
                    runSubgraphBackfill("periodic").onFailure(err ->
                        log.warn("Periodic subgraph backfill failed: {}", err.getMessage())
                    )
                );
                log.info("Subgraph backfill started, interval {}ms", cfg.polygonSubgraphBackfillIntervalMs);
            })
            .onFailure(err -> log.warn("Initial subgraph backfill failed: {}", err.getMessage()));
    }

    private Future<Void> runSubgraphBackfill(String trigger) {
        if (!backfillInFlight.compareAndSet(false, true)) {
            return Future.succeededFuture();
        }
        Future<Void> f = getLastProcessedTimestamp()
            .compose(lastTs -> {
                long timestampGt = Math.max(cfg.polygonSubgraphMinTimestamp, lastTs != null ? lastTs : 0L);
                return fetchOrderFilledEventsPage(timestampGt);
            })
            .compose(page -> processPagePipelined(page, trigger))
            .onComplete(ar -> backfillInFlight.set(false))
            .onFailure(err -> log.warn("Subgraph backfill failed: {}", err.getMessage()));
        return f.recover(err -> Future.<Void>succeededFuture());
    }

    /**
     * Process pages with pipelining: overlap insert+cursor-save with fetch of next page.
     * Bulk insert via executeBatch per page.
     */
    private Future<Void> processPagePipelined(List<JsonObject> page, String trigger) {
        if (page.isEmpty()) return Future.succeededFuture();
        List<Tuple> batch = new ArrayList<>();
        for (JsonObject e : page) {
            Tuple t = subgraphEventToTuple(e);
            if (t != null) batch.add(t);
        }
        if (batch.isEmpty()) return Future.succeededFuture();
        long maxTs = page.stream()
            .mapToLong(r -> parseLong(r.getString("timestamp"), 0L))
            .max().orElse(0L);
        Future<Void> insertAndSave = pool.preparedQuery(INSERT_TRADE).executeBatch(batch)
            .map(rs -> {
                int inserted = rs.rowCount();
                if (inserted > 0) lastInserted.addAndGet(inserted);
                return (Void) null;
            })
            .compose(v -> pool.preparedQuery(UPSERT_SYNC_STATE).execute(Tuple.of(JOB_NAME, maxTs)).map(rs -> (Void) null));
        Future<List<JsonObject>> fetchNext = fetchOrderFilledEventsPage(maxTs);
        return CompositeFuture.all(insertAndSave, fetchNext)
            .compose(cf -> {
                List<JsonObject> nextPage = (List<JsonObject>) cf.resultAt(1);
                if (!nextPage.isEmpty()) {
                    return processPagePipelined(nextPage, trigger);
                }
                return Future.succeededFuture();
            });
    }

    private Future<Long> getLastProcessedTimestamp() {
        return pool.preparedQuery(GET_LAST_TIMESTAMP).execute(Tuple.of(JOB_NAME))
            .compose(rs -> {
                if (rs.iterator().hasNext()) {
                    Long v = rs.iterator().next().getLong(0);
                    return Future.succeededFuture(v != null ? v : 0L);
                }
                return Future.succeededFuture(0L);
            });
    }

    private Future<List<JsonObject>> fetchOrderFilledEventsPage(long timestampGt) {
        return fetchOrderFilledEventsPageOnce(timestampGt, 0);
    }

    private Future<List<JsonObject>> fetchOrderFilledEventsPageOnce(long timestampGt, int attempt) {
        String query = """
            query { orderFilledEvents(first: %d, where: { timestamp_gt: "%d" }, orderBy: timestamp, orderDirection: asc) {
                id transactionHash timestamp orderHash maker taker makerAssetId takerAssetId makerAmountFilled takerAmountFilled fee
            } }
            """.formatted(cfg.polygonSubgraphPageSize, timestampGt);
        JsonObject body = new JsonObject().put("query", query);
        return webClient.postAbs(cfg.polymarketSubgraphUrl)
            .sendJson(body)
            .compose(resp -> {
                int code = resp.statusCode();
                if (code >= 502 && code <= 504 && attempt < SUBGRAPH_RETRIES) {
                    log.info("Subgraph {} retry {}/{} in {}ms", code, attempt + 1, SUBGRAPH_RETRIES, SUBGRAPH_RETRY_DELAY_MS);
                    return Future.future(p -> vertx.setTimer(SUBGRAPH_RETRY_DELAY_MS, id ->
                        fetchOrderFilledEventsPageOnce(timestampGt, attempt + 1).onComplete(ar -> {
                            if (ar.succeeded()) p.complete(ar.result());
                            else p.fail(ar.cause());
                        })
                    ));
                }
                if (code != 200) {
                    return Future.failedFuture("Subgraph HTTP " + code);
                }
                JsonObject json = resp.bodyAsJsonObject();
                JsonObject data = json.getJsonObject("data");
                if (data == null) {
                    JsonArray errs = json.getJsonArray("errors");
                    String msg = errs != null && !errs.isEmpty() ? errs.encode() : "No data";
                    return Future.failedFuture("Subgraph error: " + msg);
                }
                JsonArray events = data.getJsonArray("orderFilledEvents");
                List<JsonObject> list = new ArrayList<>();
                if (events != null) {
                    for (int i = 0; i < events.size(); i++) {
                        list.add(events.getJsonObject(i));
                    }
                }
                return Future.succeededFuture(list);
            });
    }

    private Tuple subgraphEventToTuple(JsonObject e) {
        String orderHash = e.getString("orderHash");
        String txHash = e.getString("transactionHash");
        String maker = e.getString("maker");
        String taker = e.getString("taker");
        String makerAssetId = e.getString("makerAssetId");
        String takerAssetId = e.getString("takerAssetId");
        String makerAmount = e.getString("makerAmountFilled");
        String takerAmount = e.getString("takerAmountFilled");
        String fee = e.getString("fee");
        String timestamp = e.getString("timestamp");
        if (orderHash == null || orderHash.isBlank() || txHash == null || timestamp == null) return null;
        long ts = parseLong(timestamp, 0L);
        long blockNumber = 0L;
        int logIndex = Math.abs(orderHash.hashCode()) & 0x7FFFFFFF;
        String contractAddress = cfg.polygonContractAddress;
        BigDecimal makerAmt = toBigDecimal(makerAmount);
        BigDecimal takerAmt = toBigDecimal(takerAmount);
        String tokenId = ("0".equals(makerAssetId) || makerAssetId == null || makerAssetId.isBlank())
            ? takerAssetId : makerAssetId;
        BigDecimal shares = "0".equals(makerAssetId) ? takerAmt : makerAmt;
        BigDecimal price = shares.signum() != 0
            ? ("0".equals(makerAssetId) ? makerAmt.divide(shares, 18, RoundingMode.HALF_UP) : takerAmt.divide(shares, 18, RoundingMode.HALF_UP))
            : BigDecimal.ZERO;
        String side = "0".equals(makerAssetId) ? "BUY" : "SELL";
        return Tuple.of(
            orderHash, blockNumber, (double) ts, txHash, logIndex, contractAddress,
            maker != null ? maker : "", taker != null ? taker : "",
            toBigDecimal(makerAssetId), toBigDecimal(takerAssetId),
            makerAmt, takerAmt, fee != null ? new BigDecimal(fee) : null,
            null, tokenId, price, shares, side
        );
    }

    private static long parseLong(String s, long def) {
        if (s == null || s.isBlank()) return def;
        try { return Long.parseLong(s.trim()); } catch (NumberFormatException e) { return def; }
    }

    private void logTradesInserted() {
        long currentInserted = lastInserted.get();
        long previousInserted = lastLoggedInserted.getAndSet(currentInserted);
        long deltaInserted = currentInserted - previousInserted;

        long currentDropped = droppedTrades.get();
        long previousDropped = lastLoggedDropped.getAndSet(currentDropped);
        long deltaDropped = currentDropped - previousDropped;

        int queueSize;
        synchronized (pendingTrades) {
            queueSize = pendingTrades.size();
        }

        if (deltaInserted == 0 && deltaDropped == 0) {
            log.warn("No new trades in last 10 minutes. totalInserted={}, totalDropped={}, queueSize={}",
                    currentInserted, currentDropped, queueSize);
        } else {
            log.info("Trades inserted +{} (total {}), dropped +{} (total {}), queueSize={}",
                    deltaInserted, currentInserted, deltaDropped, currentDropped, queueSize);
        }
    }

    private void connectWebSocket() {
        if (stopping.get()) {
            return;
        }

        pool.query("""
            SELECT token_id FROM polygon_market_tokens
            UNION
            SELECT yes_token_id FROM polygon_markets WHERE yes_token_id IS NOT NULL
            UNION
            SELECT no_token_id FROM polygon_markets WHERE no_token_id IS NOT NULL
        """)
        .execute()
        .onFailure(err -> {
            log.warn("Failed loading token ids: {}", err.getMessage());
            scheduleReconnect();
        })
        .onSuccess(rows -> {
            Set<String> deduped = new LinkedHashSet<>();

            rows.forEach(row -> {
                String id = row.getString(0);
                if (id != null && !id.isBlank()) {
                    deduped.add(id);
                }
            });

            List<String> tokenIds = new ArrayList<>(deduped);

            if (tokenIds.isEmpty()) {
                log.warn("No tokens found for WS subscription");
                scheduleReconnect();
                return;
            }

            ParsedWsUrl parsed;
            try {
                parsed = parseWsUrl(cfg.polymarketWsUrl);
            } catch (Exception e) {
                log.warn("Invalid WebSocket URL '{}': {}", cfg.polymarketWsUrl, e.getMessage());
                scheduleReconnect();
                return;
            }

            log.info("Connecting WebSocket host={}, port={}, path={}, tokenCount={}",
                    parsed.host(), parsed.port(), parsed.path(), tokenIds.size());

            WebSocketConnectOptions opts = new WebSocketConnectOptions()
                    .setHost(parsed.host())
                    .setPort(parsed.port())
                    .setURI(parsed.path())
                    .setSsl(parsed.port() == 443)
                    .putHeader("User-Agent", "trade-flux-polygon/1.0")
                    .putHeader("Origin", "https://polymarket.com");
            httpClient.webSocket(opts)
                .onFailure(err -> {
                    log.warn("WebSocket connect failed: {}", err.getMessage());
                    scheduleReconnect();
                })
                .onSuccess(socket -> {
                    reconnectScheduled.set(false);
                    ws = socket;

                    log.info("WebSocket connected");

                    ws.textMessageHandler(this::onWsMessage);

                    ws.closeHandler(v -> {
                        log.warn("WebSocket closed");
                        ws = null;
                        cancelTimer(pingTimerId);
                        scheduleReconnect();
                    });

                    ws.exceptionHandler(err ->
                        log.warn("WebSocket error: {}", err.getMessage())
                    );

                    subscribeTokens(tokenIds);
                    startHeartbeat();
                });
        });
    }

    private void startHeartbeat() {
        cancelTimer(pingTimerId);

        pingTimerId = vertx.setPeriodic(WS_HEARTBEAT_MS, id -> {
            try {
                if (ws != null && !ws.isClosed()) {
                    ws.writeTextMessage("PING");
                }
            } catch (Exception e) {
                log.debug("Heartbeat send failed: {}", e.getMessage());
            }
        });
    }

    private void scheduleReconnect() {
        if (stopping.get()) {
            return;
        }

        if (!reconnectScheduled.compareAndSet(false, true)) {
            return;
        }

        reconnectTimerId = vertx.setTimer(safeDelay(WS_RECONNECT_DELAY), id -> {
            reconnectScheduled.set(false);
            connectWebSocket();
        });
    }

    private void subscribeTokens(List<String> tokens) {
        if (ws == null || ws.isClosed()) {
            log.warn("WebSocket unavailable during subscription");
            return;
        }

        for (int i = 0; i < tokens.size(); i += WS_SUB_BATCH) {
            int end = Math.min(i + WS_SUB_BATCH, tokens.size());

            JsonObject msg = new JsonObject()
                .put("type", "market")
                .put("assets_ids", new JsonArray(tokens.subList(i, end)))
                .put("custom_feature_enabled", true);

            String payload = msg.encode();
            ws.writeTextMessage(payload);

            log.info("Subscribed batch {}..{} (size={})", i, end, end - i);
            log.debug("WS subscribe payload: {}", payload);
        }
    }

    private void onWsMessage(String text) {
        if (text == null || text.isBlank()
                || "{}".equals(text.trim())
                || "PONG".equals(text.trim())) {
            return;
        }

        try {
            JsonObject msg = new JsonObject(text);

            if (!"last_trade_price".equals(msg.getString("event_type"))) {
                return;
            }

            Tuple tuple = toTradeTuple(msg);
            if (tuple == null) {
                return;
            }

            if (!enqueueTrade(tuple)) {
                long dropped = droppedTrades.incrementAndGet();
                if (dropped % 1000 == 0) {
                    log.warn("Dropped {} websocket trades because queue is full", dropped);
                }
            }
        } catch (Exception e) {
            log.trace("WS parse error: {}", e.getMessage());
        }
    }

    private Tuple toTradeTuple(JsonObject msg) {
        String assetId = msg.getString("asset_id");
        String market = msg.getString("market");
        String price = msg.getString("price");
        String size = msg.getString("size");
        String side = msg.getString("side");
        String timestamp = msg.getString("timestamp");
        String txHash = msg.getString("transaction_hash");

        if (assetId == null || assetId.isBlank() || timestamp == null || timestamp.isBlank()) {
            return null;
        }

        long tsSeconds;
        try {
            long rawTs = Long.parseLong(timestamp);
            tsSeconds = rawTs > 9_999_999_999L ? rawTs / 1000L : rawTs;
        } catch (Exception e) {
            return null;
        }

        String orderHash = syntheticOrderHash(txHash, assetId, timestamp);

        return Tuple.of(
            orderHash,                   // order_hash
            0L,                          // block_number
            (double) tsSeconds,          // block_timestamp epoch seconds
            txHash,                      // transaction_hash
            0,                           // log_index
            "",                          // contract_address
            "",                          // maker
            "",                          // taker
            toBigDecimal(assetId),       // maker_asset_id
            toBigDecimal(assetId),       // taker_asset_id
            BigDecimal.ZERO,             // maker_amount
            toBigDecimal(size),          // taker_amount
            null,                        // fee
            market,                      // condition_id
            assetId,                     // token_id
            toBigDecimal(price),         // price
            toBigDecimal(size),          // shares
            side                         // side
        );
    }

    private boolean enqueueTrade(Tuple tuple) {
        synchronized (pendingTrades) {
            if (pendingTrades.size() >= MAX_PENDING_QUEUE) {
                return false;
            }
            pendingTrades.offer(tuple);
            return true;
        }
    }

    private void flushPendingTrades() {
        if (!flushInFlight.compareAndSet(false, true)) {
            return;
        }

        List<Tuple> batch = new ArrayList<>(BATCH);

        synchronized (pendingTrades) {
            while (batch.size() < BATCH && !pendingTrades.isEmpty()) {
                Tuple tuple = pendingTrades.poll();
                if (tuple != null) {
                    batch.add(tuple);
                }
            }
        }

        if (batch.isEmpty()) {
            flushInFlight.set(false);
            return;
        }

        pool.preparedQuery(INSERT_TRADE)
            .executeBatch(batch)
            .onSuccess(rs -> {
                try {
                    int inserted = rs.rowCount();
                    if (inserted > 0) {
                        lastInserted.addAndGet(inserted);
                    }
                } finally {
                    flushInFlight.set(false);

                    boolean hasMore;
                    synchronized (pendingTrades) {
                        hasMore = !pendingTrades.isEmpty();
                    }

                    if (hasMore) {
                        vertx.runOnContext(v -> flushPendingTrades());
                    }
                }
            })
            .onFailure(err -> {
                log.warn("Batch insert failed for {} trades: {}", batch.size(), err.getMessage());

                synchronized (pendingTrades) {
                    for (Tuple tuple : batch) {
                        if (pendingTrades.size() >= MAX_PENDING_QUEUE) {
                            droppedTrades.incrementAndGet();
                        } else {
                            pendingTrades.offer(tuple);
                        }
                    }
                }

                flushInFlight.set(false);
            });
    }

    private static String syntheticOrderHash(String txHash, String assetId, String ts) {
        try {
            String base = (txHash == null ? "" : txHash) + "|" + assetId + "|" + ts;
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(base.getBytes(StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder("ws-");
            for (byte b : h) {
                sb.append(String.format("%02x", b));
            }

            return sb.substring(0, Math.min(sb.length(), 66));
        } catch (Exception e) {
            return "ws-" + Math.abs(((txHash == null ? "" : txHash) + assetId + ts).hashCode());
        }
    }

    private static BigDecimal toBigDecimal(String s) {
        if (s == null || s.isBlank()) {
            return BigDecimal.ZERO;
        }

        try {
            return new BigDecimal(s);
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private static long safeDelay(long delayMs) {
        return Math.max(1L, delayMs);
    }

    private void cancelTimer(Long timerId) {
        if (timerId != null) {
            vertx.cancelTimer(timerId);
        }
    }

    private static ParsedWsUrl parseWsUrl(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("WebSocket URL is blank");
        }

        String clean = url.replaceFirst("^wss?://", "");
        int slash = clean.indexOf('/');

        String host;
        String path;

        if (slash < 0) {
            host = clean;
            path = "/";
        } else {
            host = clean.substring(0, slash);
            path = clean.substring(slash);
        }

        if (host.isBlank()) {
            throw new IllegalArgumentException("WebSocket host is blank");
        }

        int port = url.startsWith("wss://") ? 443 : 80;
        return new ParsedWsUrl(host, path, port);
    }

    private record ParsedWsUrl(String host, String path, int port) {
    }
}