package com.holdcrunch.polygon;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fetches market metadata from Polymarket Gamma API and writes to polygon_markets,
 * polygon_market_details, polygon_token_condition, polygon_market_tokens.
 * Run before trades ingestion so reports can join trades to markets.
 */
public class PolymarketMarketsService extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(PolymarketMarketsService.class);
    private static final String UPSERT_MARKET = """
        INSERT INTO polygon_market_details (condition_id, slug, question, tags, resolved_on_timestamp, closed_time, is_sports, sport_name, league_name, category_group, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
        ON CONFLICT (condition_id) DO UPDATE SET
          slug = EXCLUDED.slug,
          question = EXCLUDED.question,
          tags = EXCLUDED.tags,
          resolved_on_timestamp = COALESCE(EXCLUDED.resolved_on_timestamp, polygon_market_details.resolved_on_timestamp),
          closed_time = EXCLUDED.closed_time,
          is_sports = EXCLUDED.is_sports,
          sport_name = EXCLUDED.sport_name,
          league_name = EXCLUDED.league_name,
          category_group = EXCLUDED.category_group,
          updated_at = CURRENT_TIMESTAMP
        """;
    private static final String UPSERT_TOKEN_CONDITION = """
        INSERT INTO polygon_token_condition (token_id, condition_id)
        VALUES ($1, $2)
        ON CONFLICT (token_id) DO UPDATE SET condition_id = EXCLUDED.condition_id
        """;
    private static final String UPSERT_POLYGON_MARKET = """
        INSERT INTO polygon_markets (condition_id, slug, question, yes_token_id, no_token_id, updated_at, is_resolved, resolved_at, is_sports, sport_name, category_group, tags_json, volume)
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (condition_id) DO UPDATE SET
          slug = EXCLUDED.slug, question = EXCLUDED.question, yes_token_id = COALESCE(EXCLUDED.yes_token_id, polygon_markets.yes_token_id),
          no_token_id = COALESCE(EXCLUDED.no_token_id, polygon_markets.no_token_id), updated_at = CURRENT_TIMESTAMP,
          is_resolved = EXCLUDED.is_resolved, resolved_at = COALESCE(EXCLUDED.resolved_at, polygon_markets.resolved_at),
          is_sports = EXCLUDED.is_sports, sport_name = EXCLUDED.sport_name, category_group = EXCLUDED.category_group, tags_json = EXCLUDED.tags_json,
          volume = EXCLUDED.volume
        """;
    private static final String UPSERT_MARKET_TOKEN = """
        INSERT INTO polygon_market_tokens (token_id, condition_id, outcome)
        VALUES ($1, $2, $3)
        ON CONFLICT (token_id) DO UPDATE SET condition_id = EXCLUDED.condition_id, outcome = EXCLUDED.outcome
        """;
    private static final int TOKEN_BATCH = 30;

    public static final String ADDRESS_TRIGGER_SYNC = "polymarket.markets.triggerSync";
    public static final String ADDRESS_REFRESH_BY_SLUG = "polymarket.markets.refreshBySlug";

    private final Config cfg;
    private final WebClient webClient;
    private final PgPool pool;
    private final AtomicBoolean inFlight = new AtomicBoolean(false);
    private Long timerId;
    private Long resolutionRefreshTimerId;

    public PolymarketMarketsService(Config cfg, WebClient webClient, PgPool pool) {
        this.cfg = cfg;
        this.webClient = webClient;
        this.pool = pool;
    }

    @Override
    public void start(Promise<Void> p) {
        log.info("Polymarket markets: ingest all markets first, then interval={}ms", cfg.polymarketMarketsIntervalMs);
        vertx.eventBus().consumer(ADDRESS_TRIGGER_SYNC, msg -> {
            log.info("Manual markets sync triggered");
            runSync("manual").onComplete(ar -> {
                if (ar.succeeded()) msg.reply("ok");
                else msg.fail(500, ar.cause().getMessage());
            });
        });
        vertx.eventBus().consumer(ADDRESS_REFRESH_BY_SLUG, msg -> {
            String slug = msg.body() != null ? msg.body().toString() : null;
            if (slug == null || slug.isBlank()) { msg.fail(400, "slug required"); return; }
            refreshMarketBySlug(slug).onComplete(ar -> {
                if (ar.succeeded()) msg.reply(ar.result());
                else msg.fail(500, ar.cause().getMessage());
            });
        });
        runSync("initial").onComplete(ar -> {
            if (ar.failed()) log.warn("Initial markets sync failed: {}", ar.cause().getMessage());
            timerId = vertx.setPeriodic(cfg.polymarketMarketsIntervalMs, id -> run("periodic"));
            if (cfg.rabbitHost != null && !cfg.rabbitHost.isEmpty()) {
                resolutionRefreshTimerId = vertx.setPeriodic(cfg.polygonResolutionRefreshIntervalMs, id -> refreshResolutionForUnresolvedMarkets("resolution"));
                log.info("Resolution refresh: every {}ms", cfg.polygonResolutionRefreshIntervalMs);
                vertx.setTimer(1, id -> refreshResolutionForUnresolvedMarkets("initial"));
            }
            p.complete();
        });
    }

    @Override
    public void stop(Promise<Void> p) {
        if (timerId != null) vertx.cancelTimer(timerId);
        if (resolutionRefreshTimerId != null) vertx.cancelTimer(resolutionRefreshTimerId);
        p.complete();
    }

    private void run(String trigger) {
        if (!inFlight.compareAndSet(false, true)) return;
        runSync(trigger).onComplete(ar -> {
            inFlight.set(false);
            if (ar.failed()) log.warn("Polymarket markets sync failed ({}): {}", trigger, ar.cause().getMessage());
        });
    }

    private Future<Void> runSync(String trigger) {
        return getTokenIdsFromDb()
                .compose(ids -> {
                    if (ids.isEmpty()) return fetchBootstrapTokenIds();
                    return Future.succeededFuture(ids);
                })
                .compose(ids -> {
                    if (ids.isEmpty()) {
                        log.info("Polymarket markets {}: sync completed (no unsynced tokens)", trigger);
                        return Future.succeededFuture();
                    }
                    log.info("Polymarket markets {}: fetching details for {} unsynced token IDs", trigger, ids.size());
                    final int tokenCount = ids.size();
                    return fetchAndUpsertBatchByBatch(ids).compose(marketsSynced -> {
                        log.info("Polymarket markets {}: sync completed — {} token IDs, {} markets synced", trigger, tokenCount, marketsSynced);
                        return Future.succeededFuture();
                    });
                });
    }

    /** Publish eligible (stuck) markets to Rabbit for resolution refresh. */
    private Future<Void> refreshResolutionForUnresolvedMarkets(String trigger) {
        if (cfg.rabbitHost == null || cfg.rabbitHost.isEmpty()) {
            log.debug("Resolution refresh skipped: RABBIT_HOST not set");
            return Future.succeededFuture();
        }
        if (cfg.rabbitPassword == null || cfg.rabbitPassword.isEmpty()) {
            log.warn("Resolution refresh skipped: RABBIT_PASSWORD not set");
            return Future.succeededFuture();
        }
        return getEligibleSlugsWithCooldown()
                .compose(slugs -> publishSlugsToRabbit(trigger, slugs));
    }

    /** Eligibility: is_resolved=false, slug set, and (never in cooldown OR cooldown elapsed). No cap. */
    private Future<List<String>> getEligibleSlugsWithCooldown() {
        int cooldownHours = Math.max(0, cfg.polygonResolutionCheckCooldownHours);
        return pool.preparedQuery("""
            SELECT m.slug FROM polygon_markets m
            LEFT JOIN polygon_resolution_check_cooldown c ON m.slug = c.slug
            WHERE m.is_resolved = false AND m.slug IS NOT NULL AND m.slug != ''
              AND (c.slug IS NULL OR c.last_check_at < NOW() - ($1 || ' hours')::interval)
            ORDER BY m.updated_at ASC
            """).execute(Tuple.of(String.valueOf(cooldownHours)))
                .map(rs -> {
                    List<String> list = new ArrayList<>();
                    rs.forEach(row -> list.add(row.getString(0)));
                    return list;
                })
                .recover(e -> {
                    log.warn("Eligibility query failed (e.g. polygon_resolution_check_cooldown missing): {}", e.getMessage());
                    return Future.succeededFuture(List.of());
                });
    }

    private Future<Void> publishSlugsToRabbit(String trigger, List<String> slugs) {
        if (slugs.isEmpty()) {
            log.debug("Resolution refresh {}: 0 eligible slugs (all resolved, or all in cooldown, or none with slug)", trigger);
            return Future.succeededFuture();
        }
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(cfg.rabbitHost);
        factory.setPort(cfg.rabbitPort);
        factory.setUsername(cfg.rabbitUser);
        factory.setPassword(cfg.rabbitPassword);
        factory.setVirtualHost(cfg.rabbitVhost);
        try { factory.useSslProtocol(); } catch (Exception e) { /* AWS MQ uses SSL */ }
        Promise<Void> p = Promise.promise();
        vertx.executeBlocking(block -> {
            try (Connection conn = factory.newConnection();
                 Channel ch = conn.createChannel()) {
                ch.queueDeclare(cfg.rabbitQueueResolutionRefresh, true, false, false, null);
                for (String slug : slugs) {
                    ch.basicPublish("", cfg.rabbitQueueResolutionRefresh, null, slug.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
                block.complete();
            } catch (Exception e) {
                block.fail(e);
            }
        }, false).onComplete(ar -> {
            if (ar.failed()) {
                log.warn("Polymarket markets {}: failed to publish {} slugs to Rabbit: {}", trigger, slugs.size(), ar.cause().getMessage());
                p.fail(ar.cause());
            } else {
                upsertCooldown(slugs).onComplete(ca -> {
                    if (ca.failed()) log.warn("Failed to upsert resolution cooldown: {}", ca.cause().getMessage());
                    log.info("Polymarket markets {}: published {} slugs to Rabbit for resolution refresh", trigger, slugs.size());
                    p.complete();
                });
            }
        });
        return p.future();
    }

    private Future<Void> upsertCooldown(List<String> slugs) {
        if (slugs.isEmpty()) return Future.succeededFuture();
        List<Tuple> rows = new ArrayList<>();
        for (String slug : slugs) rows.add(Tuple.of(slug));
        return pool.preparedQuery("""
            INSERT INTO polygon_resolution_check_cooldown (slug, last_check_at, check_count)
            VALUES ($1, CURRENT_TIMESTAMP, 1)
            ON CONFLICT (slug) DO UPDATE SET last_check_at = CURRENT_TIMESTAMP, check_count = polygon_resolution_check_cooldown.check_count + 1
            """)
                .executeBatch(rows)
                .map(rs -> (Void) null)
                .recover(e -> Future.succeededFuture());
    }

    /** Fetch Gamma by slug and upsert only when umaResolutionStatus=resolved. Used by Rabbit consumer. */
    public Future<Integer> refreshMarketBySlug(String slug) {
        String base = cfg.polymarketGammaApiUrl.replaceAll("/$", "");
        String path = base + "/markets/slug/" + java.net.URLEncoder.encode(slug, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
        return webClient.getAbs(path).timeout(15_000).send()
                .compose(r -> {
                    if (r.statusCode() != 200) return Future.succeededFuture(0);
                    try {
                        JsonObject m = r.bodyAsJsonObject();
                        if (m == null) return Future.succeededFuture(0);
                        String uma = m.getString("umaResolutionStatus");
                        if (!"resolved".equalsIgnoreCase(uma != null ? uma.trim() : "")) return Future.succeededFuture(0);
                        return upsertMarketsAndTokens(new JsonArray().add(m));
                    } catch (Exception e) {
                        return Future.succeededFuture(0);
                    }
                })
                .recover(e -> Future.succeededFuture(0));
    }

    /** Token IDs from chain/trades that we haven't yet fetched market details for (resume-safe). */
    private Future<List<String>> getTokenIdsFromDb() {
        return pool.query("""
            SELECT DISTINCT t.token_id FROM (
                SELECT token_id FROM polygon_chain_tokens
                UNION
                SELECT DISTINCT token_id FROM polygon_trades WHERE token_id IS NOT NULL AND token_id != ''
            ) t
            LEFT JOIN polygon_token_condition p ON t.token_id = p.token_id
            WHERE p.token_id IS NULL
            """).execute()
                .map(rs -> {
                    List<String> list = new ArrayList<>();
                    rs.forEach(row -> list.add(row.getString(0)));
                    return list;
                })
                .recover(e -> Future.succeededFuture(List.<String>of()));
    }

    /** Page size for Gamma bootstrap (API often caps around 500). */
    private static final int BOOTSTRAP_PAGE_SIZE = 500;

    /** Bootstrap: fetch all active, non-closed markets via pagination (limit + offset). */
    private Future<List<String>> fetchBootstrapTokenIds() {
        return fetchBootstrapTokenIdsPage(0, new ArrayList<>());
    }

    private Future<List<String>> fetchBootstrapTokenIdsPage(int offset, List<String> accumulatedIds) {
        String base = cfg.polymarketGammaApiUrl.replaceAll("/$", "");
        String path = base + "/markets?active=true&closed=false&limit=" + BOOTSTRAP_PAGE_SIZE + "&offset=" + offset;
        return webClient.getAbs(path).timeout(60_000).send()
                .compose(r -> {
                    if (r.statusCode() != 200) return Future.succeededFuture(accumulatedIds);
                    JsonArray arr = parseBodyAsJsonArray(r);
                    if (arr == null || arr.isEmpty()) return Future.succeededFuture(accumulatedIds);
                    for (int i = 0; i < arr.size(); i++) {
                        JsonObject m = arr.getJsonObject(i);
                        JsonArray tokens = asJsonArray(m.getValue("clobTokenIds"));
                        if (tokens != null) for (int j = 0; j < tokens.size(); j++) accumulatedIds.add(tokens.getString(j));
                    }
                    int received = arr.size();
                    log.debug("Bootstrap: page offset={}, received {} markets, total token IDs so far={}", offset, received, accumulatedIds.size());
                    if (received < BOOTSTRAP_PAGE_SIZE)
                        return filterUnsynced(accumulatedIds);
                    Promise<List<String>> nextPromise = Promise.promise();
                    vertx.setTimer(200, id -> fetchBootstrapTokenIdsPage(offset + received, accumulatedIds).onComplete(ar -> { if (ar.succeeded()) nextPromise.complete(ar.result()); else nextPromise.fail(ar.cause()); }));
                    return nextPromise.future();
                })
                .recover(e -> Future.succeededFuture(accumulatedIds));
    }

    private static final int FILTER_BATCH = 200;

    private Future<List<String>> filterUnsynced(List<String> ids) {
        if (ids.isEmpty()) return Future.succeededFuture(List.of());
        return filterUnsyncedChunk(ids, 0, new ArrayList<>());
    }

    private Future<List<String>> filterUnsyncedChunk(List<String> ids, int offset, List<String> unsynced) {
        if (offset >= ids.size()) return Future.succeededFuture(unsynced);
        int end = Math.min(offset + FILTER_BATCH, ids.size());
        List<String> chunk = new ArrayList<>(ids.subList(offset, end));
        String placeholders = String.join(",", java.util.stream.IntStream.range(1, chunk.size() + 1).mapToObj(i -> "$" + i).toList());
        Tuple tuple = Tuple.from(chunk);
        return pool.preparedQuery("SELECT token_id FROM polygon_token_condition WHERE token_id IN (" + placeholders + ")")
                .execute(tuple)
                .map(rs -> {
                    var synced = new java.util.HashSet<String>();
                    rs.forEach(row -> synced.add(row.getString(0)));
                    chunk.stream().filter(id -> !synced.contains(id)).forEach(unsynced::add);
                    return unsynced;
                })
                .compose(result -> filterUnsyncedChunk(ids, end, result))
                .recover(e -> Future.succeededFuture(ids));
    }

    /** Fetch and upsert batch-by-batch so progress persists on crash (resume-safe). Returns total markets synced. Iterative to avoid deep Future chains and OOM. */
    private Future<Integer> fetchAndUpsertBatchByBatch(List<String> tokenIds) {
        Promise<Integer> promise = Promise.promise();
        fetchAndUpsertBatchIterative(tokenIds, 0, 0, promise);
        return promise.future();
    }

    private void fetchAndUpsertBatchIterative(List<String> tokenIds, int offset, int marketsSoFar, Promise<Integer> promise) {
        if (offset >= tokenIds.size()) {
            promise.complete(marketsSoFar);
            return;
        }
        List<String> batch = tokenIds.subList(offset, Math.min(offset + TOKEN_BATCH, tokenIds.size()));
        fetchMarketsByTokenIds(batch)
                .compose(this::upsertMarketsAndTokens)
                .onComplete(ar -> {
                    if (ar.failed()) {
                        promise.fail(ar.cause());
                        return;
                    }
                    int nextOffset = offset + batch.size();
                    int newTotal = marketsSoFar + ar.result();
                    if (nextOffset >= tokenIds.size()) {
                        promise.complete(newTotal);
                    } else {
                        vertx.setTimer(1, id -> fetchAndUpsertBatchIterative(tokenIds, nextOffset, newTotal, promise));
                    }
                });
    }

    private Future<JsonArray> fetchMarketsByTokenIds(List<String> tokenIds) {
        String base = cfg.polymarketGammaApiUrl.replaceAll("/$", "");
        String path = base + "/markets?include_tag=true&limit=" + (tokenIds.size() * 2);
        for (String id : tokenIds) path += "&clob_token_ids=" + id;
        return webClient.getAbs(path).timeout(60_000).send()
                .compose(r -> {
                    if (r.statusCode() != 200) return Future.failedFuture("Gamma API " + r.statusCode());
                    JsonArray arr = parseBodyAsJsonArray(r);
                    return Future.succeededFuture(arr != null ? arr : new JsonArray());
                });
    }

    /** Safely get JsonArray from value that may be JsonArray, JSON string, or null. */
    private static JsonArray asJsonArray(Object v) {
        if (v == null) return null;
        if (v instanceof JsonArray ja) return ja;
        if (v instanceof String s) {
            s = s.trim();
            if (s.startsWith("[")) try { return new JsonArray(s); } catch (Exception e) { return null; }
        }
        return null;
    }

    /** Gamma API may return array, single object, or string; avoid ClassCastException. */
    private static JsonArray parseBodyAsJsonArray(HttpResponse<?> r) {
        try {
            String body = r.bodyAsString();
            if (body == null || body.isBlank()) return new JsonArray();
            body = body.trim();
            if (body.startsWith("[")) return new JsonArray(body);
            if (body.startsWith("{")) return new JsonArray().add(new JsonObject(body));
            log.debug("Gamma API body not JSON array/object: {}", body.length() > 100 ? body.substring(0, 100) + "..." : body);
            return new JsonArray();
        } catch (Exception e) {
            log.debug("Gamma API parse failed: {}", e.getMessage());
            return new JsonArray();
        }
    }

    private static final String LOAD_CATEGORY_MAPPING = "SELECT category_key, is_sports, tag_prefix FROM polygon_category_mapping";

    /** Row from polygon_category_mapping: is_sports and tag_prefix = full tags value (e.g. sports,esports or sports,nhl). */
    private record CategoryMappingRow(boolean isSports, String tagPrefix) {}

    /** Upsert markets and related rows; returns number of markets in this batch. */
    private Future<Integer> upsertMarketsAndTokens(JsonArray markets) {
        if (markets.isEmpty()) return Future.succeededFuture(0);
        return pool.query(LOAD_CATEGORY_MAPPING).execute()
                .map(rs -> {
                    Map<String, CategoryMappingRow> map = new HashMap<>();
                    Set<String> sportsKeys = new HashSet<>();
                    rs.forEach(row -> {
                        String key = row.getString(0).toLowerCase().trim();
                        boolean isSports = Boolean.TRUE.equals(row.getBoolean(1));
                        String tagPrefix = row.getString(2);
                        if (tagPrefix != null) tagPrefix = tagPrefix.trim();
                        map.put(key, new CategoryMappingRow(isSports, tagPrefix));
                        if (isSports) sportsKeys.add(key);
                    });
                    return new Object[] { map, sportsKeys };
                })
                .recover(e -> Future.succeededFuture(new Object[] { Map.<String, CategoryMappingRow>of(), Set.<String>of() }))
                .compose(pair -> {
                    @SuppressWarnings("unchecked")
                    Map<String, CategoryMappingRow> categoryMap = (Map<String, CategoryMappingRow>) ((Object[]) pair)[0];
                    @SuppressWarnings("unchecked")
                    Set<String> sportsKeys = (Set<String>) ((Object[]) pair)[1];
                    List<Tuple> detailsRows = new ArrayList<>();
                    List<Tuple> tokenConditionRows = new ArrayList<>();
                    List<Tuple> polygonMarketsRows = new ArrayList<>();
                    List<Tuple> marketTokensRows = new ArrayList<>();
                    for (int i = 0; i < markets.size(); i++) {
                        JsonObject m = markets.getJsonObject(i);
                        String conditionId = m.getString("conditionId");
                        if (conditionId == null || conditionId.isBlank()) continue;
                        String slug = m.getString("slug");
                        String question = m.getString("question");
                        String tagsStr = tagsToCommaSeparated(asJsonArray(m.getValue("tags")));
                        String closedTime = m.getString("closedTime");
                        String umaResolutionStatus = m.getString("umaResolutionStatus");
                        boolean isResolved = "resolved".equalsIgnoreCase(umaResolutionStatus != null ? umaResolutionStatus.trim() : "");
                        OffsetDateTime closedTimeParsed = parseClosedTime(closedTime);
                        OffsetDateTime resolvedAt = isResolved ? closedTimeParsed : null;
                        String categoryGroup = firstNonBlank(m.getString("categoryGroup"), m.getString("category"));
                        boolean isSports = (tagsStr != null && !tagsStr.isBlank())
                                ? tagsStr.toLowerCase().contains("sports")
                                : isSportsFromMapping(m, slug, categoryGroup, sportsKeys, categoryMap);
                        String sportName = isSports ? "sports" : null;
                        if (tagsStr == null || tagsStr.isBlank()) {
                            tagsStr = inferredTagsJson(categoryGroup, isSports, slug, categoryMap);
                        }
                        String leagueName = leagueFromMarket(m, isSports);
                        String yesTokenId = null, noTokenId = null;
                        JsonArray outcomes = asJsonArray(m.getValue("outcomes"));
                        JsonArray tokens = asJsonArray(m.getValue("clobTokenIds"));
                        if (outcomes != null && tokens != null && outcomes.size() == tokens.size()) {
                            for (int j = 0; j < outcomes.size(); j++) {
                                String out = outcomes.getString(j);
                                String tid = tokens.getString(j);
                                if ("Yes".equalsIgnoreCase(out)) yesTokenId = tid; else if ("No".equalsIgnoreCase(out)) noTokenId = tid;
                                if (tid != null) {
                                    tokenConditionRows.add(Tuple.of(tid, conditionId));
                                    marketTokensRows.add(Tuple.of(tid, conditionId, "Yes".equalsIgnoreCase(out) ? "YES" : "NO"));
                                }
                            }
                        }
                        BigDecimal volume = parseVolume(m);
                        detailsRows.add(Tuple.of(conditionId, slug, question, tagsStr, closedTimeParsed, closedTime, isSports, sportName, leagueName, categoryGroup));
                        polygonMarketsRows.add(Tuple.of(conditionId, slug, question, yesTokenId, noTokenId, isResolved, resolvedAt, isSports, sportName, categoryGroup, tagsStr, volume));
                    }
                    final int count = detailsRows.size();
                    return pool.preparedQuery(UPSERT_MARKET).executeBatch(detailsRows)
                            .compose(v -> pool.preparedQuery(UPSERT_TOKEN_CONDITION).executeBatch(tokenConditionRows))
                            .compose(v -> pool.preparedQuery(UPSERT_POLYGON_MARKET).executeBatch(polygonMarketsRows))
                            .compose(v -> pool.preparedQuery(UPSERT_MARKET_TOKEN).executeBatch(marketTokensRows))
                            .map(v -> count);
                });
    }

    /** When Gamma gives no tags, set tags_json from category_group or from mapping (tag_prefix is full value e.g. sports,esports). */
    private static String inferredTagsJson(String categoryGroup, boolean isSports, String slug, Map<String, CategoryMappingRow> categoryMap) {
        if (categoryGroup != null && !categoryGroup.isBlank()) return categoryGroup.trim();
        if (slug != null && slug.contains("-")) {
            String prefix = slug.split("-")[0].trim().toLowerCase();
            if (!prefix.isEmpty() && categoryMap != null) {
                CategoryMappingRow row = categoryMap.get(prefix);
                if (row != null && row.tagPrefix != null && !row.tagPrefix.isBlank()) return row.tagPrefix;
            }
        }
        if (!isSports) return null;
        if (slug != null && slug.contains("-")) {
            String prefix = slug.split("-")[0].trim().toLowerCase();
            if (!prefix.isEmpty()) return "sports," + prefix;
        }
        return "sports";
    }

    /** When market has no tags, treat as sports if category/series/slug prefix is in polygon_category_mapping (is_sports=true). */
    private static boolean isSportsFromMapping(JsonObject m, String slug, String categoryGroup, Set<String> sportsKeys, Map<String, CategoryMappingRow> categoryMap) {
        if (slug != null && slug.contains("-")) {
            String prefix = slug.split("-")[0].trim().toLowerCase();
            if (!prefix.isEmpty() && categoryMap != null) {
                CategoryMappingRow row = categoryMap.get(prefix);
                if (row != null) return row.isSports;
            }
        }
        if (sportsKeys.isEmpty()) return false;
        String cat = categoryGroup != null ? categoryGroup.trim().toLowerCase() : null;
        if (cat != null && !cat.isEmpty() && sportsKeys.contains(cat)) return true;
        if (cat != null && sportsKeys.contains(cat.replace(' ', '-'))) return true;
        JsonArray events = asJsonArray(m.getValue("events"));
        if (events != null && !events.isEmpty()) {
            Object first = events.getValue(0);
            if (first instanceof JsonObject ev) {
                String seriesSlug = firstNonBlank(ev.getString("seriesSlug"), ev.getString("slug"));
                if (seriesSlug != null && sportsKeys.contains(seriesSlug.trim().toLowerCase())) return true;
            }
        }
        if (slug != null && slug.contains("-")) {
            String prefix = slug.split("-")[0].trim().toLowerCase();
            if (!prefix.isEmpty() && sportsKeys.contains(prefix)) return true;
        }
        return false;
    }

    private static String tagsToCommaSeparated(JsonArray tags) {
        if (tags == null) return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tags.size(); i++) {
            if (i > 0) sb.append(",");
            Object t = tags.getValue(i);
            if (t instanceof JsonObject jo) sb.append(jo.getString("slug", jo.getString("label", "")));
            else sb.append(t);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) if (v != null && !v.isBlank()) return v;
        return null;
    }

    /** Gamma: closedTime often "2020-11-02 16:31:01+00" (space, +00). Used as resolvedAt when resolved. */
    private static final DateTimeFormatter CLOSED_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");

    /** Gamma: volumeNum (number) or volume (string). */
    private static BigDecimal parseVolume(JsonObject m) {
        Object v = m.getValue("volumeNum");
        if (v instanceof Number n) return BigDecimal.valueOf(n.doubleValue());
        if (v != null) {
            try { return new BigDecimal(v.toString().trim()); } catch (Exception e) { }
        }
        String s = m.getString("volume");
        if (s == null || s.isBlank()) return null;
        try { return new BigDecimal(s.trim()); } catch (Exception e) { return null; }
    }

    private static OffsetDateTime parseClosedTime(String s) {
        if (s == null || s.isBlank()) return null;
        s = s.trim();
        try { return OffsetDateTime.parse(s); } catch (DateTimeParseException e1) {
            try { return Instant.parse(s).atOffset(ZoneOffset.UTC); } catch (Exception e2) {
                try {
                    String normalized = s.replace(" ", "T");
                    if (s.endsWith("+00") && !s.endsWith("+00:00")) normalized = normalized.replace("+00", "+00:00");
                    else if (s.endsWith("-00")) normalized = normalized.replace("-00", "-00:00");
                    return OffsetDateTime.parse(normalized);
                } catch (Exception e3) {
                    try { return OffsetDateTime.parse(s, CLOSED_TIME_FORMAT); } catch (Exception e4) { return null; }
                }
            }
        }
    }

    /** From nested events[0] (e.g. "World Series Champion 2024") when sports. */
    private static String leagueFromMarket(JsonObject m, boolean isSports) {
        if (!isSports) return null;
        JsonArray events = asJsonArray(m.getValue("events"));
        if (events == null || events.isEmpty()) return null;
        Object first = events.getValue(0);
        if (first instanceof JsonObject ev)
            return firstNonBlank(ev.getString("title"), ev.getString("slug"), ev.getString("ticker"));
        return null;
    }

    /** Parse ISO-8601 timestamp (e.g. updatedAt "2026-03-12T08:53:06.027112Z"). */
    private static OffsetDateTime parseIsoTimestamp(String s) {
        if (s == null || s.isBlank()) return null;
        try { return OffsetDateTime.parse(s.trim()); } catch (DateTimeParseException e) {
            try { return Instant.parse(s.trim()).atOffset(ZoneOffset.UTC); } catch (Exception e2) { return null; }
        }
    }

    private static OffsetDateTime parseResolved(JsonObject m) {
        for (String key : new String[] { "resolvedAt", "closedTime" }) {
            Object r = m.getValue(key);
            if (r == null) continue;
            if (r instanceof String s) {
                s = s.trim();
                if (s.isEmpty()) continue;
                try { return OffsetDateTime.parse(s); } catch (DateTimeParseException e1) {
                    try { return Instant.parse(s).atOffset(ZoneOffset.UTC); } catch (Exception e2) {
                        try {
                            String normalized = s.replace(" ", "T");
                            if (s.endsWith("+00") && !s.endsWith("+00:00")) normalized = normalized.replace("+00", "+00:00");
                            else if (s.endsWith("-00")) normalized = normalized.replace("-00", "-00:00");
                            return OffsetDateTime.parse(normalized);
                        } catch (Exception e3) {
                            try { return OffsetDateTime.parse(s, CLOSED_TIME_FORMAT); } catch (Exception e4) { /* next key */ }
                        }
                    }
                }
            }
        }
        return null;
    }
}
