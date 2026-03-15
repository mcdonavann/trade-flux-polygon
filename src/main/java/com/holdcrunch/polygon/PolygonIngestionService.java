package com.holdcrunch.polygon;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
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
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PolygonIngestionService extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(PolygonIngestionService.class);

    private static final int BATCH = 200;

    // Keep this small to avoid large websocket subscribe frames.
    private static final int WS_SUB_BATCH = 50;

    private static final long WS_SUB_DELAY_MS = 200L;
    private static final long WS_HEARTBEAT_MS = 10_000L;
    private static final long WS_RECONNECT_DELAY_MS = 5_000L;
    private static final long WS_RECONNECT_MAX_DELAY_MS = 120_000L;
    private static final int WS_MAX_FRAME_SIZE = 1024 * 1024;

    private static final long TRADES_LOG_INTERVAL_MS = 10 * 60 * 1000L;
    private static final long FLUSH_INTERVAL_MS = 250L;
    private static final int MAX_PENDING_QUEUE = 50_000;

    private static final int SUBGRAPH_RETRIES = 3;
    private static final long SUBGRAPH_RETRY_DELAY_MS = 2_000L;

    private static final String JOB_NAME = "polygon_trades";

    /**
     * Note:
     * sync_state.last_processed_block is being used as a timestamp cursor here.
     * The column name is historical and kept to avoid forcing a migration.
     */
    private static final String GET_LAST_TIMESTAMP = """
        SELECT last_processed_block
        FROM sync_state
        WHERE job_name = $1
        """;

    private static final String UPSERT_SYNC_STATE = """
        INSERT INTO sync_state (job_name, last_processed_block, last_success_at)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (job_name)
        DO UPDATE SET
            last_processed_block = EXCLUDED.last_processed_block,
            last_success_at = CURRENT_TIMESTAMP
        """;

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
        ON CONFLICT DO NOTHING
        """;

    /**
     * Sports-only websocket subscriptions (and subgraph backfill token set).
     *
     * polygon_markets: yes_token_id, no_token_id, is_sports, is_resolved, resolved_at.
     * Include only unresolved sports markets. When resolved, tokens drop out and are unsubscribed on next refresh.
     * resolved_at IS NULL: include (Gamma often omits it; treat as not yet confirmed resolved).
     */
    private static final String LOAD_SPORTS_WS_TOKENS = """
        SELECT yes_token_id AS token_id
        FROM polygon_markets
        WHERE yes_token_id IS NOT NULL
          AND is_sports = TRUE
          AND (is_resolved = FALSE OR resolved_at IS NULL)

        UNION

        SELECT no_token_id AS token_id
        FROM polygon_markets
        WHERE no_token_id IS NOT NULL
          AND is_sports = TRUE
          AND (is_resolved = FALSE OR resolved_at IS NULL)
        """;

    private static final String LOAD_SUBSCRIBED_TOKENS = "SELECT token_id FROM polygon_ws_subscribed_tokens";
    private static final String INSERT_SUBSCRIBED_TOKEN = "INSERT INTO polygon_ws_subscribed_tokens (token_id) VALUES ($1) ON CONFLICT (token_id) DO NOTHING";
    private static final String DELETE_ONE_SUBSCRIBED_TOKEN = "DELETE FROM polygon_ws_subscribed_tokens WHERE token_id = $1";

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
    private Long wsRefreshTimerId;

    private final AtomicLong lastInserted = new AtomicLong(0);
    private final AtomicLong lastLoggedInserted = new AtomicLong(0);
    private final AtomicLong droppedTrades = new AtomicLong(0);
    private final AtomicLong lastLoggedDropped = new AtomicLong(0);
    private final AtomicLong wsReconnectAttempts = new AtomicLong(0);

    private final AtomicBoolean flushInFlight = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean backfillInFlight = new AtomicBoolean(false);

    private final Queue<Tuple> pendingTrades = new ArrayDeque<>();

    public PolygonIngestionService(Config cfg, WebClient webClient, PgPool pool) {
        this.cfg = cfg;
        this.webClient = webClient;
        this.pool = pool;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        log.info("Polygon ingestion starting");

        HttpClientOptions opts = new HttpClientOptions()
            .setMaxWebSocketFrameSize(WS_MAX_FRAME_SIZE)
            .setMaxWebSocketMessageSize(WS_MAX_FRAME_SIZE)
            .setKeepAlive(true);

        httpClient = vertx.createHttpClient(opts);

        flushTimerId = vertx.setPeriodic(FLUSH_INTERVAL_MS, id -> flushPendingTrades());
        tradesLogTimerId = vertx.setPeriodic(TRADES_LOG_INTERVAL_MS, id -> logTradesInserted());

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
        cancelTimer(wsRefreshTimerId);

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
                    runSubgraphBackfill("periodic")
                        .onFailure(err -> log.warn("Periodic subgraph backfill failed: {}", err.getMessage()))
                );
                log.info("Subgraph backfill started, interval={}ms", cfg.polygonSubgraphBackfillIntervalMs);
            })
            .onFailure(err -> log.warn("Initial subgraph backfill failed: {}", err.getMessage()));
    }

    /**
     * Subgraph backfill: sports-only (same token set as WebSocket).
     * Complements WS when it drops by re-scanning the overlap window; only sports trades are inserted.
     */
    private Future<Void> runSubgraphBackfill(String trigger) {
        if (!backfillInFlight.compareAndSet(false, true)) {
            return Future.succeededFuture();
        }

        return getLastProcessedTimestamp()
            .compose(lastTs -> loadSportsWsTokenIds().map(ids -> {
                long cursorTs = lastTs != null ? lastTs : 0L;
                long overlapSeconds = Math.max(0L, cfg.polygonSubgraphOverlapSeconds);
                long resumeTs = Math.max(
                    cfg.polygonSubgraphMinTimestamp,
                    Math.max(0L, cursorTs - overlapSeconds)
                );
                Set<String> sportsTokenIds = new HashSet<>(ids);
                log.debug("Running subgraph backfill trigger={}, cursorTs={}, resumeTs={}, sportsTokens={}",
                    trigger, cursorTs, resumeTs, sportsTokenIds.size());
                return new Object[] { resumeTs, sportsTokenIds };
            }))
            .compose(pair -> {
                Object[] p = (Object[]) pair;
                long resumeTs = (Long) p[0];
                @SuppressWarnings("unchecked")
                Set<String> sportsTokenIds = (Set<String>) p[1];
                return fetchOrderFilledEventsPage(resumeTs)
                    .compose(page -> processPagePipelined(page, trigger, sportsTokenIds));
            })
            .onFailure(err -> log.warn("Subgraph backfill failed: {}", err.getMessage()))
            .onComplete(ar -> backfillInFlight.set(false))
            .recover(err -> Future.succeededFuture());
    }

    /**
     * Process pages with pipelining:
     * - insert current page (sports-only when sportsTokenIds is non-null)
     * - save cursor
     * - fetch next page in parallel
     */
    private Future<Void> processPagePipelined(List<JsonObject> page, String trigger, Set<String> sportsTokenIds) {
        if (page.isEmpty()) {
            return Future.succeededFuture();
        }

        List<Tuple> batch = new ArrayList<>(page.size());
        for (JsonObject e : page) {
            Tuple t = subgraphEventToTuple(e, sportsTokenIds);
            if (t != null) {
                batch.add(t);
            }
        }

        long maxTs = page.stream()
            .mapToLong(r -> parseLong(r.getString("timestamp"), 0L))
            .max()
            .orElse(0L);

        // Advance cursor even when batch is empty (page had no sports trades) so we don't get stuck
        Future<Void> insertAndSave = batch.isEmpty()
            ? pool.preparedQuery(UPSERT_SYNC_STATE).execute(Tuple.of(JOB_NAME, maxTs)).map(rs -> (Void) null)
            : pool.preparedQuery(INSERT_TRADE)
                .executeBatch(batch)
                .map(rs -> {
                    int inserted = rs.rowCount();
                    if (inserted > 0) {
                        lastInserted.addAndGet(inserted);
                    }
                    return (Void) null;
                })
                .compose(v ->
                    pool.preparedQuery(UPSERT_SYNC_STATE)
                        .execute(Tuple.of(JOB_NAME, maxTs))
                        .map(rs -> (Void) null)
                );

        Future<List<JsonObject>> fetchNext = fetchOrderFilledEventsPage(maxTs);

        return CompositeFuture.all(insertAndSave, fetchNext)
            .compose(cf -> {
                @SuppressWarnings("unchecked")
                List<JsonObject> nextPage = (List<JsonObject>) cf.resultAt(1);

                // timestamp_gte + overlap means next page may repeat rows; DB dedupe handles it.
                // Continue only if we got a full page.
                if (nextPage.size() >= cfg.polygonSubgraphPageSize) {
                    return processPagePipelined(nextPage, trigger, sportsTokenIds);
                }

                return Future.succeededFuture();
            });
    }

    private Future<Long> getLastProcessedTimestamp() {
        return pool.preparedQuery(GET_LAST_TIMESTAMP)
            .execute(Tuple.of(JOB_NAME))
            .map(rs -> {
                if (rs.iterator().hasNext()) {
                    Long v = rs.iterator().next().getLong(0);
                    return v != null ? v : 0L;
                }
                return 0L;
            });
    }

    private Future<List<JsonObject>> fetchOrderFilledEventsPage(long timestampGte) {
        return fetchOrderFilledEventsPageOnce(timestampGte, 0);
    }

    private Future<List<JsonObject>> fetchOrderFilledEventsPageOnce(long timestampGte, int attempt) {
        String query = """
            query {
              orderFilledEvents(
                first: %d,
                where: { timestamp_gte: "%d" },
                orderBy: timestamp,
                orderDirection: asc
              ) {
                id
                transactionHash
                timestamp
                orderHash
                maker
                taker
                makerAssetId
                takerAssetId
                makerAmountFilled
                takerAmountFilled
                fee
              }
            }
            """.formatted(cfg.polygonSubgraphPageSize, timestampGte);

        JsonObject body = new JsonObject().put("query", query);

        return webClient.postAbs(cfg.polymarketSubgraphUrl)
            .sendJson(body)
            .compose(resp -> {
                int code = resp.statusCode();

                if (code >= 502 && code <= 504 && attempt < SUBGRAPH_RETRIES) {
                    log.info("Subgraph HTTP {} retry {}/{} in {}ms",
                        code, attempt + 1, SUBGRAPH_RETRIES, SUBGRAPH_RETRY_DELAY_MS);

                    Promise<List<JsonObject>> retryPromise = Promise.promise();
                    vertx.setTimer(SUBGRAPH_RETRY_DELAY_MS, id ->
                        fetchOrderFilledEventsPageOnce(timestampGte, attempt + 1)
                            .onSuccess(retryPromise::complete)
                            .onFailure(retryPromise::fail)
                    );
                    return retryPromise.future();
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
                        JsonObject item = events.getJsonObject(i);
                        if (item != null) {
                            list.add(item);
                        }
                    }
                }

                return Future.succeededFuture(list);
            });
    }

    private Tuple subgraphEventToTuple(JsonObject e, Set<String> sportsTokenIds) {
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

        if (orderHash == null || orderHash.isBlank() || txHash == null || txHash.isBlank() || timestamp == null || timestamp.isBlank()) {
            return null;
        }

        BigDecimal makerAmt = toBigDecimal(makerAmount);
        BigDecimal takerAmt = toBigDecimal(takerAmount);

        String tokenId = ("0".equals(makerAssetId) || makerAssetId == null || makerAssetId.isBlank())
            ? takerAssetId
            : makerAssetId;

        // Sports-only backfill: skip events whose token is not in the sports set
        if (sportsTokenIds != null && !sportsTokenIds.contains(tokenId)) {
            return null;
        }

        long ts = parseLong(timestamp, 0L);
        long blockNumber = 0L;
        int logIndex = Math.abs(orderHash.hashCode()) & 0x7FFFFFFF;
        String contractAddress = cfg.polygonContractAddress;

        BigDecimal shares = "0".equals(makerAssetId) ? takerAmt : makerAmt;

        BigDecimal price = shares.signum() != 0
            ? ("0".equals(makerAssetId)
                ? makerAmt.divide(shares, 18, RoundingMode.HALF_UP)
                : takerAmt.divide(shares, 18, RoundingMode.HALF_UP))
            : BigDecimal.ZERO;

        String side = "0".equals(makerAssetId) ? "BUY" : "SELL";

        return Tuple.of(
            orderHash,
            blockNumber,
            (double) ts,
            txHash,
            logIndex,
            contractAddress,
            maker != null ? maker : "",
            taker != null ? taker : "",
            toBigDecimal(makerAssetId),
            toBigDecimal(takerAssetId),
            makerAmt,
            takerAmt,
            fee != null && !fee.isBlank() ? new BigDecimal(fee) : null,
            null,
            tokenId,
            price,
            shares,
            side
        );
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

        loadSportsWsTokenIds()
            .onFailure(err -> {
                log.warn("Failed loading sports websocket token ids: {}", err.getMessage());
                scheduleReconnect();
            })
            .onSuccess(tokenIds -> {
                if (tokenIds.isEmpty()) {
                    log.warn("No sports tokens found for websocket subscription");
                    scheduleReconnect();
                    return;
                }

                ParsedWsUrl parsed;
                try {
                    parsed = parseWsUrl(cfg.polymarketWsUrl);
                } catch (Exception e) {
                    log.warn("Invalid websocket URL '{}': {}", cfg.polymarketWsUrl, e.getMessage());
                    scheduleReconnect();
                    return;
                }

                log.info("Connecting websocket host={}, port={}, path={}, sportsTokenCount={}",
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
                        wsReconnectAttempts.set(0);
                        ws = socket;

                        log.info("WebSocket connected");

                        ws.textMessageHandler(this::onWsMessage);

                        ws.closeHandler(v -> {
                            log.warn("WebSocket closed");
                            ws = null;
                            cancelTimer(pingTimerId);
                            cancelTimer(wsRefreshTimerId);
                            scheduleReconnect();
                        });

                        ws.exceptionHandler(err ->
                            log.warn("WebSocket error: {}", err.getMessage())
                        );

                        subscribeTokens(tokenIds);
                        persistSubscribedTokens(tokenIds);
                        startHeartbeat();
                        startWsRefreshTimer();
                    });
            });
    }

    private Future<List<String>> loadSportsWsTokenIds() {
        return pool.preparedQuery(LOAD_SPORTS_WS_TOKENS)
            .execute()
            .map(rows -> {
                Set<String> deduped = new LinkedHashSet<>();
                for (Row row : rows) {
                    String tokenId = row.getString("token_id");
                    if (tokenId != null && !tokenId.isBlank()) {
                        deduped.add(tokenId);
                    }
                }
                return new ArrayList<>(deduped);
            });
    }

    private Future<Set<String>> loadSubscribedTokenIds() {
        return pool.preparedQuery(LOAD_SUBSCRIBED_TOKENS)
            .execute()
            .map(rows -> {
                Set<String> set = new HashSet<>();
                for (Row row : rows) {
                    String tokenId = row.getString("token_id");
                    if (tokenId != null && !tokenId.isBlank()) {
                        set.add(tokenId);
                    }
                }
                return set;
            });
    }

    /** Replace persisted subscribed tokens with the set we just subscribed to (e.g. on connect). */
    private void persistSubscribedTokens(List<String> tokenIds) {
        if (tokenIds == null || tokenIds.isEmpty()) {
            return;
        }
        pool.preparedQuery("TRUNCATE polygon_ws_subscribed_tokens").execute()
            .onFailure(err -> log.warn("Failed to truncate polygon_ws_subscribed_tokens: {}", err.getMessage()))
            .onSuccess(v -> {
                List<Tuple> batch = tokenIds.stream().map(Tuple::of).toList();
                pool.preparedQuery(INSERT_SUBSCRIBED_TOKEN).executeBatch(batch)
                    .onFailure(err -> log.warn("Failed to persist subscribed tokens: {}", err.getMessage()))
                    .onSuccess(r -> log.debug("Persisted {} subscribed token ids", tokenIds.size()));
            });
    }

    private void startWsRefreshTimer() {
        cancelTimer(wsRefreshTimerId);
        long intervalMs = Math.max(60_000L, cfg.polygonWsSubscriptionsRefreshIntervalMs);
        wsRefreshTimerId = vertx.setPeriodic(intervalMs, id -> refreshWsSubscriptions());
        log.info("WS subscriptions refresh started, interval={}ms", intervalMs);
    }

    /**
     * Subscribe to new sports tokens and unsubscribe from tokens no longer in the sports set (e.g. resolved past lookback).
     * Updates polygon_ws_subscribed_tokens to match.
     */
    private void refreshWsSubscriptions() {
        if (ws == null || ws.isClosed() || stopping.get()) {
            return;
        }

        CompositeFuture.join(loadSportsWsTokenIds(), loadSubscribedTokenIds())
            .onFailure(err -> log.warn("Failed to load tokens for WS refresh: {}", err.getMessage()))
            .onSuccess(cf -> {
                @SuppressWarnings("unchecked")
                List<String> sportsList = (List<String>) cf.resultAt(0);
                @SuppressWarnings("unchecked")
                Set<String> subscribed = (Set<String>) cf.resultAt(1);
                Set<String> sports = new HashSet<>(sportsList);

                Set<String> toUnsubscribe = new HashSet<>(subscribed);
                toUnsubscribe.removeAll(sports);
                Set<String> toSubscribe = new HashSet<>(sports);
                toSubscribe.removeAll(subscribed);

                if (toUnsubscribe.isEmpty() && toSubscribe.isEmpty()) {
                    return;
                }

                if (!toUnsubscribe.isEmpty()) {
                    List<String> unsubList = new ArrayList<>(toUnsubscribe);
                    sendUnsubscribeBatches(unsubList);
                    List<Tuple> deleteBatch = unsubList.stream().map(Tuple::of).toList();
                    pool.preparedQuery(DELETE_ONE_SUBSCRIBED_TOKEN).executeBatch(deleteBatch)
                        .onFailure(err -> log.warn("Failed to delete unsubscribed tokens: {}", err.getMessage()))
                        .onSuccess(r -> log.info("Unsubscribed {} tokens (resolved or no longer sports), removed from persisted set", unsubList.size()));
                }

                if (!toSubscribe.isEmpty()) {
                    List<String> subList = new ArrayList<>(toSubscribe);
                    subscribeTokens(subList);
                    List<Tuple> insertBatch = subList.stream().map(Tuple::of).toList();
                    pool.preparedQuery(INSERT_SUBSCRIBED_TOKEN).executeBatch(insertBatch)
                        .onFailure(err -> log.warn("Failed to persist newly subscribed tokens: {}", err.getMessage()))
                        .onSuccess(r -> log.info("Subscribed {} new sports tokens, persisted", subList.size()));
                }
            });
    }

    private void sendUnsubscribeBatches(List<String> tokenIds) {
        if (tokenIds == null || tokenIds.isEmpty() || ws == null || ws.isClosed()) {
            return;
        }
        for (int i = 0; i < tokenIds.size(); i += WS_SUB_BATCH) {
            int start = i;
            int end = Math.min(i + WS_SUB_BATCH, tokenIds.size());
            List<String> batch = tokenIds.subList(start, end);
            long delayMs = (i / WS_SUB_BATCH) * WS_SUB_DELAY_MS;
            vertx.setTimer(safeDelay(delayMs), id -> {
                if (ws == null || ws.isClosed()) return;
                JsonObject msg = new JsonObject()
                    .put("type", "market")
                    .put("assets_ids", new JsonArray(batch))
                    .put("operation", "unsubscribe");
                ws.writeTextMessage(msg.encode());
                log.info("Unsubscribed batch {}..{} size={}", start, end, batch.size());
            });
        }
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

        long attempt = wsReconnectAttempts.getAndIncrement();
        long exponentialDelay = WS_RECONNECT_DELAY_MS * (1L << Math.min(attempt, 5));
        long delay = Math.min(exponentialDelay, WS_RECONNECT_MAX_DELAY_MS);

        if (attempt > 0) {
            log.info("WebSocket reconnect attempt {}, delay={}ms", attempt + 1, delay);
        }

        reconnectTimerId = vertx.setTimer(safeDelay(delay), id -> {
            reconnectScheduled.set(false);
            connectWebSocket();
        });
    }

    private void subscribeTokens(List<String> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            return;
        }

        if (ws == null || ws.isClosed()) {
            log.warn("WebSocket unavailable during subscription");
            return;
        }

        int batchIndex = 0;
        for (int i = 0; i < tokens.size(); i += WS_SUB_BATCH) {
            int start = i;
            int end = Math.min(i + WS_SUB_BATCH, tokens.size());
            List<String> batchTokens = new ArrayList<>(tokens.subList(start, end));
            long delayMs = batchIndex * WS_SUB_DELAY_MS;
            batchIndex++;

            vertx.setTimer(safeDelay(delayMs), id -> {
                if (ws == null || ws.isClosed()) {
                    return;
                }

                JsonObject msg = new JsonObject()
                    .put("type", "market")
                    .put("assets_ids", new JsonArray(batchTokens))
                    .put("custom_feature_enabled", true);

                String payload = msg.encode();
                int payloadBytes = payload.getBytes(StandardCharsets.UTF_8).length;

                ws.writeTextMessage(payload);

                log.debug("Subscribed batch {}..{} size={} payloadBytes={}",
                    start, end, batchTokens.size(), payloadBytes);
                log.debug("WS subscribe payload: {}", payload);
            });
        }
    }

    private void onWsMessage(String text) {
        if (text == null || text.isBlank()) {
            return;
        }

        String trimmed = text.trim();
        if ("{}".equals(trimmed) || "PONG".equals(trimmed)) {
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
            orderHash,
            0L,
            (double) tsSeconds,
            txHash,
            0,
            "",
            "",
            "",
            toBigDecimal(assetId),
            toBigDecimal(assetId),
            BigDecimal.ZERO,
            toBigDecimal(size),
            null,
            market,
            assetId,
            toBigDecimal(price),
            toBigDecimal(size),
            side
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

    private static long parseLong(String s, long def) {
        if (s == null || s.isBlank()) {
            return def;
        }
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return def;
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

    private static String syntheticOrderHash(String txHash, String assetId, String ts) {
        try {
            String base = (txHash == null ? "" : txHash) + "|" + assetId + "|" + ts;
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(base.getBytes(StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder("ws-");
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }

            return sb.substring(0, Math.min(sb.length(), 66));
        } catch (Exception e) {
            return "ws-" + Math.abs(((txHash == null ? "" : txHash) + assetId + ts).hashCode());
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