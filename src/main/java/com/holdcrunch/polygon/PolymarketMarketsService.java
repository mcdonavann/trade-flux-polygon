package com.holdcrunch.polygon;

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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
        INSERT INTO polygon_markets (condition_id, slug, question, yes_token_id, no_token_id, updated_at, is_resolved, resolved_at, is_sports, sport_name, category_group, tags_json)
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (condition_id) DO UPDATE SET
          slug = EXCLUDED.slug, question = EXCLUDED.question, yes_token_id = COALESCE(EXCLUDED.yes_token_id, polygon_markets.yes_token_id),
          no_token_id = COALESCE(EXCLUDED.no_token_id, polygon_markets.no_token_id), updated_at = CURRENT_TIMESTAMP,
          is_resolved = EXCLUDED.is_resolved, resolved_at = COALESCE(EXCLUDED.resolved_at, polygon_markets.resolved_at),
          is_sports = EXCLUDED.is_sports, sport_name = EXCLUDED.sport_name, category_group = EXCLUDED.category_group, tags_json = EXCLUDED.tags_json
        """;
    private static final String UPSERT_MARKET_TOKEN = """
        INSERT INTO polygon_market_tokens (token_id, condition_id, outcome)
        VALUES ($1, $2, $3)
        ON CONFLICT (token_id) DO UPDATE SET condition_id = EXCLUDED.condition_id, outcome = EXCLUDED.outcome
        """;
    private static final int TOKEN_BATCH = 30;

    public static final String ADDRESS_TRIGGER_SYNC = "polymarket.markets.triggerSync";

    private final Config cfg;
    private final WebClient webClient;
    private final PgPool pool;
    private final AtomicBoolean inFlight = new AtomicBoolean(false);
    private Long timerId;

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
        runSync("initial").onComplete(ar -> {
            if (ar.failed()) log.warn("Initial markets sync failed: {}", ar.cause().getMessage());
            timerId = vertx.setPeriodic(cfg.polymarketMarketsIntervalMs, id -> run("periodic"));
            p.complete();
        });
    }

    @Override
    public void stop(Promise<Void> p) {
        if (timerId != null) vertx.cancelTimer(timerId);
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
                })
                .compose(v -> refreshResolutionForUnresolvedMarkets(trigger));
    }

    /** Re-fetch up to N markets with is_resolved=false by slug from Gamma and upsert to refresh is_resolved/resolved_at. */
    private Future<Void> refreshResolutionForUnresolvedMarkets(String trigger) {
        if (cfg.polymarketMarketsResolutionRefreshLimit <= 0) return Future.succeededFuture();
        return getUnresolvedMarketSlugs(cfg.polymarketMarketsResolutionRefreshLimit)
                .compose(slugs -> {
                    if (slugs.isEmpty()) return Future.succeededFuture();
                    log.info("Polymarket markets {}: refreshing resolution for {} markets (is_resolved=false)", trigger, slugs.size());
                    return refreshResolutionBySlugs(slugs, 0, 0);
                })
                .compose(updated -> {
                    if (updated > 0) log.info("Polymarket markets {}: resolution refresh updated {} markets", trigger, updated);
                    return Future.succeededFuture();
                });
    }

    private Future<List<String>> getUnresolvedMarketSlugs(int limit) {
        return pool.preparedQuery("SELECT slug FROM polygon_markets WHERE is_resolved = false AND slug IS NOT NULL AND slug != '' ORDER BY updated_at ASC LIMIT $1")
                .execute(Tuple.of(limit))
                .map(rs -> {
                    List<String> list = new ArrayList<>();
                    rs.forEach(row -> list.add(row.getString(0)));
                    return list;
                })
                .recover(e -> Future.succeededFuture(List.of()));
    }

    private Future<Integer> refreshResolutionBySlugs(List<String> slugs, int index, int updatedSoFar) {
        if (index >= slugs.size()) return Future.succeededFuture(updatedSoFar);
        String slug = slugs.get(index);
        String base = cfg.polymarketGammaApiUrl.replaceAll("/$", "");
        String path = base + "/markets/slug/" + java.net.URLEncoder.encode(slug, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
        return webClient.getAbs(path).timeout(15_000).send()
                .compose(r -> {
                    if (r.statusCode() != 200) return Future.succeededFuture(updatedSoFar);
                    try {
                        JsonObject m = r.bodyAsJsonObject();
                        if (m == null) return Future.succeededFuture(updatedSoFar);
                        return upsertMarketsAndTokens(new JsonArray().add(m))
                                .map(n -> updatedSoFar + (n > 0 ? 1 : 0));
                    } catch (Exception e) {
                        return Future.succeededFuture(updatedSoFar);
                    }
                })
                .recover(e -> Future.succeededFuture(updatedSoFar))
                .compose(newUpdated -> refreshResolutionBySlugs(slugs, index + 1, newUpdated));
    }

    /** Token IDs from chain that we haven't yet fetched market details for (resume-safe). */
    private Future<List<String>> getTokenIdsFromDb() {
        return pool.query("""
            SELECT c.token_id FROM polygon_chain_tokens c
            LEFT JOIN polygon_token_condition p ON c.token_id = p.token_id
            WHERE p.token_id IS NULL
            """).execute()
                .map(rs -> {
                    List<String> list = new ArrayList<>();
                    rs.forEach(row -> list.add(row.getString(0)));
                    return list;
                })
                .recover(e -> Future.succeededFuture(List.<String>of()));
    }

    private Future<List<String>> fetchBootstrapTokenIds() {
        String path = cfg.polymarketGammaApiUrl.replaceAll("/$", "") + "/markets?limit=2000";
        return webClient.getAbs(path).timeout(60_000).send()
                .compose(r -> {
                    if (r.statusCode() != 200) return Future.succeededFuture(List.<String>of());
                    JsonArray arr = parseBodyAsJsonArray(r);
                    if (arr == null || arr.isEmpty()) return Future.succeededFuture(List.<String>of());
                    List<String> ids = new ArrayList<>();
                    for (int i = 0; i < arr.size(); i++) {
                        JsonObject m = arr.getJsonObject(i);
                        JsonArray tokens = asJsonArray(m.getValue("clobTokenIds"));
                        if (tokens != null) for (int j = 0; j < tokens.size(); j++) ids.add(tokens.getString(j));
                    }
                    return filterUnsynced(ids);
                })
                .recover(e -> Future.succeededFuture(List.<String>of()));
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

    /** Fetch and upsert batch-by-batch so progress persists on crash (resume-safe). Returns total markets synced. */
    private Future<Integer> fetchAndUpsertBatchByBatch(List<String> tokenIds) {
        return fetchAndUpsertBatch(tokenIds, 0, 0);
    }

    private Future<Integer> fetchAndUpsertBatch(List<String> tokenIds, int offset, int marketsSoFar) {
        if (offset >= tokenIds.size()) return Future.succeededFuture(marketsSoFar);
        List<String> batch = tokenIds.subList(offset, Math.min(offset + TOKEN_BATCH, tokenIds.size()));
        return fetchMarketsByTokenIds(batch)
                .compose(this::upsertMarketsAndTokens)
                .compose(batchMarkets -> fetchAndUpsertBatch(tokenIds, offset + batch.size(), marketsSoFar + batchMarkets));
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

    private static final String LOAD_SPORTS_KEYS = "SELECT category_key FROM polymarket_category_mapping WHERE is_sports = true";

    /** Upsert markets and related rows; returns number of markets in this batch. */
    private Future<Integer> upsertMarketsAndTokens(JsonArray markets) {
        if (markets.isEmpty()) return Future.succeededFuture(0);
        return pool.query(LOAD_SPORTS_KEYS).execute()
                .map(rs -> {
                    Set<String> keys = new HashSet<>();
                    rs.forEach(row -> keys.add(row.getString(0).toLowerCase().trim()));
                    return keys;
                })
                .recover(e -> Future.succeededFuture(Set.<String>of()))
                .compose(sportsKeys -> {
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
                        OffsetDateTime resolvedOn = parseResolved(m);
                        String closedTime = m.getString("closedTime");
                        boolean closed = Boolean.TRUE.equals(m.getBoolean("closed"));
                        boolean hasResolvedBy = firstNonBlank(m.getString("resolvedBy")) != null;
                        boolean isResolved = closed || resolvedOn != null || hasResolvedBy;
                        OffsetDateTime resolvedAt = resolvedOn;
                        boolean isSports = (tagsStr != null && !tagsStr.isBlank())
                                ? tagsStr.toLowerCase().contains("sports")
                                : isSportsFromMapping(m, slug, firstNonBlank(m.getString("categoryGroup"), m.getString("category")), sportsKeys);
                        String sportName = isSports ? "sports" : null;
                        String categoryGroup = firstNonBlank(m.getString("categoryGroup"), m.getString("category"));
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
                        detailsRows.add(Tuple.of(conditionId, slug, question, tagsStr, resolvedOn, closedTime, isSports, sportName, leagueName, categoryGroup));
                        polygonMarketsRows.add(Tuple.of(conditionId, slug, question, yesTokenId, noTokenId, isResolved, resolvedAt, isSports, sportName, categoryGroup, tagsStr));
                    }
                    final int count = detailsRows.size();
                    return pool.preparedQuery(UPSERT_MARKET).executeBatch(detailsRows)
                            .compose(v -> pool.preparedQuery(UPSERT_TOKEN_CONDITION).executeBatch(tokenConditionRows))
                            .compose(v -> pool.preparedQuery(UPSERT_POLYGON_MARKET).executeBatch(polygonMarketsRows))
                            .compose(v -> pool.preparedQuery(UPSERT_MARKET_TOKEN).executeBatch(marketTokensRows))
                            .map(v -> count);
                });
    }

    /** When market has no tags, treat as sports if category/series/slug prefix is in polymarket_category_mapping (is_sports=true). */
    private static boolean isSportsFromMapping(JsonObject m, String slug, String categoryGroup, Set<String> sportsKeys) {
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

    /** Gamma: closedTime often "2020-11-02 16:31:01+00" (space, +00); resolvedAt rare. Normalize for parsing. */
    private static final DateTimeFormatter CLOSED_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");

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
