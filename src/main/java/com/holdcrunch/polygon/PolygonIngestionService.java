package com.holdcrunch.polygon;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ingests Polymarket trades into polygon_trades (Postgres) for SQL weekly reports.
 * <p>Startup order (orchestrated from Application):
 * <ol>
 *   <li>Ingest all markets first (PolymarketMarketsService)</li>
 *   <li>Backfill historical trades from the subgraph</li>
 *   <li>Start WebSocket listener for new trades</li>
 * </ol>
 * Polymarket API → polygon_markets; Subgraph (history) + WebSocket (live) → polygon_trades.
 */
public class PolygonIngestionService extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(PolygonIngestionService.class);

    private static final String INSERT_TRADE = """
        INSERT INTO polygon_trades (order_hash, block_number, block_timestamp, transaction_hash, log_index,
            contract_address, maker, taker, maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee, condition_id, token_id, price, shares, side)
        VALUES ($1, $2, to_timestamp($3)::timestamptz, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        ON CONFLICT (order_hash) DO NOTHING
        """;
    private static final String UPSERT_TOKEN = """
        INSERT INTO polygon_chain_tokens (token_id, first_block, last_block)
        VALUES ($1, $2, $3)
        ON CONFLICT (token_id) DO UPDATE SET
          first_block = LEAST(polygon_chain_tokens.first_block, EXCLUDED.first_block),
          last_block = GREATEST(polygon_chain_tokens.last_block, EXCLUDED.last_block),
          updated_at = CURRENT_TIMESTAMP
        """;
    private static final int BATCH = 200;
    private static final String JOB_NAME = "polygon_trades";
    private static final String GET_LAST_BLOCK = "SELECT last_processed_block FROM sync_state WHERE job_name = $1";
    private static final String UPSERT_SYNC_STATE = """
        INSERT INTO sync_state (job_name, last_processed_block, last_success_at)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (job_name) DO UPDATE SET last_processed_block = EXCLUDED.last_processed_block, last_success_at = CURRENT_TIMESTAMP
        """;

    private final Config cfg;
    private final WebClient webClient;
    private final PgPool pool;
    private static final long TRADES_LOG_INTERVAL_MS = 10 * 60 * 1000; // 10 minutes

    private final AtomicBoolean backfillInFlight = new AtomicBoolean(false);
    private final AtomicLong lastInserted = new AtomicLong(0);
    private Long backfillTimerId;
    private Long tradesLogTimerId;
    private io.vertx.core.http.WebSocket ws;
    private Long pingTimerId;

    public PolygonIngestionService(Config cfg, WebClient webClient, PgPool pool) {
        this.cfg = cfg;
        this.webClient = webClient;
        this.pool = pool;
    }

    @Override
    public void start(Promise<Void> p) {
        log.info("Polygon ingestion: subgraph backfill={}, ws live={}", cfg.polygonSubgraphBackfillEnabled, cfg.polygonWsLiveEnabled);
        if (cfg.polygonSubgraphBackfillEnabled) {
            Future<Void> backfill = fetchSubgraphIndexingStatus()
                    .compose(meta -> runSubgraphBackfill("initial"));
            backfill.onComplete(ar -> {
                if (ar.failed()) log.warn("Initial backfill failed: {}", ar.cause().getMessage());
                if (cfg.polygonWsLiveEnabled) connectWebSocket();
                backfillTimerId = vertx.setPeriodic(cfg.polygonSubgraphBackfillIntervalMs, id -> runBackfill("periodic"));
                tradesLogTimerId = vertx.setPeriodic(TRADES_LOG_INTERVAL_MS, id -> logTradesInserted());
                p.complete();
            });
        } else {
            if (cfg.polygonWsLiveEnabled) connectWebSocket();
            tradesLogTimerId = vertx.setPeriodic(TRADES_LOG_INTERVAL_MS, id -> logTradesInserted());
            p.complete();
        }
    }

    private void logTradesInserted() {
        long n = lastInserted.get();
        if (n > 0) log.info("Trades inserted (total so far): {}", n);
    }

    @Override
    public void stop(Promise<Void> p) {
        if (backfillTimerId != null) vertx.cancelTimer(backfillTimerId);
        if (tradesLogTimerId != null) vertx.cancelTimer(tradesLogTimerId);
        if (pingTimerId != null) vertx.cancelTimer(pingTimerId);
        if (ws != null) try { ws.close(); } catch (Exception ignored) { }
        p.complete();
    }

    private void runBackfill(String trigger) {
        if (!backfillInFlight.compareAndSet(false, true)) return;
        runSubgraphBackfill(trigger).onComplete(ar -> {
            backfillInFlight.set(false);
            if (ar.failed()) log.warn("Subgraph backfill failed ({}): {}", trigger, ar.cause().getMessage());
        });
    }

    private Future<Void> runSubgraphBackfill(String trigger) {
        return getLastProcessedTimestamp()
                .compose(lastTs -> {
                    long timestampGt = lastTs != null ? lastTs + 1
                            : (cfg.polygonSubgraphMinTimestamp > 0 ? cfg.polygonSubgraphMinTimestamp - 1 : 0L);
                    return fetchOrderFilledEventsPage(timestampGt)
                            .compose(page -> {
                                if (page == null || page.isEmpty()) return Future.succeededFuture();
                                if ("initial".equals(trigger)) {
                                    log.debug("Subgraph backfill initial: got {} events after timestamp {}", page.size(), timestampGt);
                                } else {
                                    log.info("Subgraph backfill {}: got {} events after timestamp {}", trigger, page.size(), timestampGt);
                                }
                                return insertTradesFromSubgraph(page)
                                        .compose(n -> {
                                            if ("initial".equals(trigger)) {
                                                log.debug("Subgraph backfill initial: inserted {} trades", n);
                                            } else {
                                                log.info("Subgraph backfill {}: inserted {} trades", trigger, n);
                                            }
                                            return upsertChainTokensFromSubgraph(page);
                                        })
                                        .compose(v -> {
                                            long maxTs = page.stream().mapToLong(r -> parseLong(r, "timestamp", 0L)).max().orElse(timestampGt);
                                            return saveSyncState(maxTs);
                                        })
                                        .compose(v -> page.size() >= cfg.polygonSubgraphPageSize
                                                ? runSubgraphBackfill(trigger)
                                                : Future.succeededFuture());
                            });
                });
    }

    private Future<Long> getLastProcessedTimestamp() {
        return pool.preparedQuery(GET_LAST_BLOCK).execute(Tuple.of(JOB_NAME))
                .compose(rs -> {
                    if (rs.iterator().hasNext()) return Future.succeededFuture(rs.iterator().next().getLong(0));
                    return Future.succeededFuture((Long) null);
                })
                .recover(e -> Future.succeededFuture(null));
    }

    private Future<Void> saveSyncState(long lastProcessedCursor) {
        return pool.preparedQuery(UPSERT_SYNC_STATE).execute(Tuple.of(JOB_NAME, lastProcessedCursor)).mapEmpty();
    }

    private Future<Void> fetchSubgraphIndexingStatus() {
        String query = """
            query SubgraphMeta {
              _meta { block { number timestamp } }
            }
            """;
        JsonObject body = new JsonObject().put("query", query);
        return webClient.postAbs(cfg.polymarketSubgraphUrl).timeout(10_000)
                .sendJsonObject(body)
                .compose(r -> {
                    if (r.statusCode() != 200) return Future.<Void>succeededFuture();
                    JsonObject j = r.bodyAsJsonObject();
                    if (j.containsKey("errors")) return Future.<Void>succeededFuture();
                    JsonObject data = j.getJsonObject("data");
                    if (data == null) return Future.<Void>succeededFuture();
                    JsonObject meta = data.getJsonObject("_meta");
                    if (meta == null) return Future.<Void>succeededFuture();
                    JsonObject block = meta.getJsonObject("block");
                    if (block == null) return Future.<Void>succeededFuture();
                    long blockNum = parseLong(block, "number", 0L);
                    long blockTs = parseLong(block, "timestamp", 0L);
                    if (blockTs > 0) {
                        String dateStr = java.time.Instant.ofEpochSecond(blockTs).atOffset(java.time.ZoneOffset.UTC).toString();
                        log.info("Subgraph indexed up to block {} (timestamp {} ~ {})", blockNum, blockTs, dateStr);
                        if (cfg.polygonSubgraphMinTimestamp > 0 && blockTs < cfg.polygonSubgraphMinTimestamp) {
                            String wantDate = java.time.Instant.ofEpochSecond(cfg.polygonSubgraphMinTimestamp).atOffset(java.time.ZoneOffset.UTC).toString();
                            log.warn("Subgraph is behind chain: indexed to {} which is before requested start {}. Trades from {} may not be available yet.", dateStr, wantDate, wantDate);
                        }
                    }
                    return Future.<Void>succeededFuture();
                })
                .recover(e -> Future.<Void>succeededFuture());
    }

    private static final int SUBGRAPH_RETRIES = 3;
    private static final long SUBGRAPH_RETRY_DELAY_MS = 2_000;

    private Future<List<JsonObject>> fetchOrderFilledEventsPage(long timestampGt) {
        return fetchOrderFilledEventsPageOnce(timestampGt, 0);
    }

    private Future<List<JsonObject>> fetchOrderFilledEventsPageOnce(long timestampGt, int attempt) {
        String query = """
            query OrderFilledEvents($first: Int!, $timestampGt: BigInt) {
              orderFilledEvents(first: $first, orderBy: timestamp, orderDirection: asc, where: { timestamp_gt: $timestampGt }) {
                id
                transactionHash
                timestamp
                maker
                taker
                makerAssetId
                takerAssetId
                makerAmountFilled
                takerAmountFilled
              }
            }
            """;
        JsonObject variables = new JsonObject().put("first", cfg.polygonSubgraphPageSize).put("timestampGt", String.valueOf(timestampGt));
        JsonObject body = new JsonObject().put("query", query).put("variables", variables);
        return webClient.postAbs(cfg.polymarketSubgraphUrl).timeout(30_000)
                .sendJsonObject(body)
                .compose(r -> {
                    if (r.statusCode() != 200) {
                        log.warn("Subgraph HTTP {}: {}", r.statusCode(), r.bodyAsString());
                        int code = r.statusCode();
                        boolean retryable = code == 502 || code == 503 || code == 504;
                        if (retryable && attempt + 1 < SUBGRAPH_RETRIES) {
                            log.info("Subgraph {} retry {}/{} in {}ms", code, attempt + 1, SUBGRAPH_RETRIES, SUBGRAPH_RETRY_DELAY_MS);
                            return Future.<List<JsonObject>>future(p -> vertx.setTimer(SUBGRAPH_RETRY_DELAY_MS, id ->
                                    fetchOrderFilledEventsPageOnce(timestampGt, attempt + 1).onComplete(ar -> {
                                        if (ar.succeeded()) p.complete(ar.result());
                                        else p.fail(ar.cause());
                                    })));
                        }
                        return Future.failedFuture("Subgraph " + code);
                    }
                    JsonObject j = r.bodyAsJsonObject();
                    if (j.containsKey("errors")) {
                        log.warn("Subgraph errors: {}", j.getValue("errors"));
                        return Future.failedFuture("Subgraph errors");
                    }
                    JsonObject data = j.getJsonObject("data");
                    if (data == null) return Future.succeededFuture(new ArrayList<JsonObject>());
                    JsonArray arr = data.getJsonArray("orderFilledEvents");
                    if (arr == null) return Future.succeededFuture(new ArrayList<JsonObject>());
                    List<JsonObject> list = new ArrayList<>();
                    for (int i = 0; i < arr.size(); i++) list.add(arr.getJsonObject(i));
                    return Future.succeededFuture(list);
                })
                .recover(e -> Future.succeededFuture(new ArrayList<JsonObject>()));
    }

    private Future<Long> insertTradesFromSubgraph(List<JsonObject> rows) {
        List<Tuple> tuples = new ArrayList<>();
        for (JsonObject r : rows) {
            String orderHash = r.getString("id");
            if (orderHash == null) continue;
            long blockTs = parseLong(r, "timestamp", 0L);
            String txHash = nullToEmpty(r.getString("transactionHash"));
            int logIndex = Math.abs(orderHash.hashCode());
            String maker = nullToEmpty(r.getString("maker"));
            String taker = nullToEmpty(r.getString("taker"));
            String makerAssetId = nullToEmpty(r.getString("makerAssetId"));
            String takerAssetId = nullToEmpty(r.getString("takerAssetId"));
            String makerAmount = nullToEmpty(r.getString("makerAmountFilled"));
            String takerAmount = nullToEmpty(r.getString("takerAmountFilled"));
            BigDecimal price = BigDecimal.ZERO;
            BigDecimal shares = toBigDecimal(takerAmount);
            String tokenId = takerAssetId.isEmpty() ? makerAssetId : takerAssetId;
            tuples.add(Tuple.of(orderHash, 0L, (double) blockTs, txHash, logIndex, "", maker, taker,
                    makerAssetId.isEmpty() ? BigDecimal.ZERO : new BigDecimal(makerAssetId),
                    takerAssetId.isEmpty() ? BigDecimal.ZERO : new BigDecimal(takerAssetId),
                    makerAmount.isEmpty() ? BigDecimal.ZERO : new BigDecimal(makerAmount),
                    takerAmount.isEmpty() ? BigDecimal.ZERO : new BigDecimal(takerAmount),
                    (BigDecimal) null, null, tokenId, price, shares, (String) null));
        }
        if (tuples.isEmpty()) return Future.succeededFuture(0L);
        return insertChunk(tuples, 0).onSuccess(lastInserted::addAndGet);
    }

    private Future<Long> insertChunk(List<Tuple> tuples, int offset) {
        if (offset >= tuples.size()) return Future.succeededFuture(0L);
        int end = Math.min(offset + BATCH, tuples.size());
        return pool.preparedQuery(INSERT_TRADE).executeBatch(tuples.subList(offset, end))
                .map(m -> {
                    long n = 0;
                    for (RowSet rs = m; rs != null; rs = rs.next()) n += rs.rowCount();
                    return n;
                })
                .compose(n -> insertChunk(tuples, end).map(n2 -> n + n2));
    }

    private Future<Void> upsertChainTokensFromSubgraph(List<JsonObject> rows) {
        List<Tuple> tuples = new ArrayList<>();
        for (JsonObject r : rows) {
            long ts = parseLong(r, "timestamp", 0L);
            String m = r.getString("makerAssetId");
            String t = r.getString("takerAssetId");
            if (m != null && !m.isEmpty()) tuples.add(Tuple.of(m, 0L, 0L));
            if (t != null && !t.isEmpty()) tuples.add(Tuple.of(t, 0L, 0L));
        }
        if (tuples.isEmpty()) return Future.succeededFuture();
        return pool.preparedQuery(UPSERT_TOKEN).executeBatch(tuples).mapEmpty();
    }

    private void connectWebSocket() {
        pool.query("SELECT token_id FROM polygon_market_tokens UNION SELECT yes_token_id FROM polygon_markets WHERE yes_token_id IS NOT NULL UNION SELECT no_token_id FROM polygon_markets WHERE no_token_id IS NOT NULL").execute()
                .onFailure(e -> {
                    log.warn("Failed to load token IDs for WebSocket: {}", e.getMessage());
                })
                .onSuccess(rs -> {
                    List<String> tokenIds = new ArrayList<>();
                    rs.forEach(row -> { String t = row.getString(0); if (t != null && !t.isEmpty()) tokenIds.add(t); });
                    if (tokenIds.isEmpty()) { log.info("No token IDs for WebSocket subscription"); return; }
                    JsonObject msg = new JsonObject().put("auth", new JsonObject()).put("type", "subscribe").put("markets", tokenIds);
                    String url = cfg.polymarketWsUrl;
                    int port = url.startsWith("wss://") ? 443 : 80;
                    String rest = url.replaceFirst("^wss?://", "");
                    int slash = rest.indexOf('/');
                    String host = slash > 0 ? rest.substring(0, slash) : rest;
                    String path = slash > 0 ? rest.substring(slash) : "/";
                    vertx.createHttpClient().webSocket(port, host, path)
                            .onFailure(e -> log.warn("WebSocket connect failed: {}", e.getMessage()))
                            .onSuccess(sock -> {
                                ws = sock;
                                ws.writeTextMessage(msg.encode());
                                ws.textMessageHandler(this::onWsMessage);
                                ws.closeHandler(v -> { ws = null; vertx.setTimer(5000L, id -> connectWebSocket()); });
                                pingTimerId = vertx.setPeriodic(25000L, id -> { if (ws != null && !ws.isClosed()) ws.writeTextMessage("PING"); });
                            });
                });
    }

    private void onWsMessage(String text) {
        if ("PONG".equals(text)) return;
        try {
            JsonObject msg = new JsonObject(text);
            if (!"last_trade_price".equals(msg.getString("event_type"))) return;
            String assetId = msg.getString("asset_id");
            String market = msg.getString("market");
            String price = msg.getString("price");
            String size = msg.getString("size");
            String side = msg.getString("side");
            String timestamp = msg.getString("timestamp");
            String txHash = msg.getString("transaction_hash");
            if (assetId == null || market == null || timestamp == null) return;
            long tsMs = Long.parseLong(timestamp);
            long tsSec = tsMs / 1000;
            if (cfg.polygonSubgraphMinTimestamp > 0 && tsSec < cfg.polygonSubgraphMinTimestamp) return;
            String orderHash = syntheticOrderHash(txHash, assetId, timestamp);
            List<Tuple> one = List.of(Tuple.of(
                    orderHash, 0L, tsMs / 1000.0, txHash != null ? txHash : "", 0, "",
                    "", "", toBigDecimal(assetId), toBigDecimal(assetId), BigDecimal.ZERO, toBigDecimal(size), (BigDecimal) null,
                    market, assetId, toBigDecimal(price), toBigDecimal(size), side));
            pool.preparedQuery(INSERT_TRADE).executeBatch(one)
                    .onSuccess(rs -> { if (rs.rowCount() > 0) lastInserted.incrementAndGet(); })
                    .onFailure(e -> log.debug("WS trade insert failed: {}", e.getMessage()));
        } catch (Exception e) { log.trace("WS message parse: {}", e.getMessage()); }
    }

    private static String syntheticOrderHash(String txHash, String assetId, String timestamp) {
        String base = (txHash != null ? txHash : "") + "|" + assetId + "|" + timestamp;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(base.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("ws-");
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.length() > 66 ? sb.substring(0, 66) : sb.toString();
        } catch (Exception e) {
            return "ws-" + Math.abs(base.hashCode()) + "-" + timestamp;
        }
    }

    private static long parseLong(JsonObject r, String key, long def) {
        Object v = r.getValue(key);
        if (v == null) return def;
        if (v instanceof Number n) return n.longValue();
        String s = String.valueOf(v);
        if (s.isEmpty()) return def;
        try { return Long.parseLong(s.trim()); } catch (NumberFormatException e) { return def; }
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }

    private static BigDecimal toBigDecimal(String s) {
        if (s == null || s.isEmpty()) return BigDecimal.ZERO;
        try { return new BigDecimal(s); } catch (NumberFormatException e) { return BigDecimal.ZERO; }
    }
}
