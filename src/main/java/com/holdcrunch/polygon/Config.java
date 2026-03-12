package com.holdcrunch.polygon;

import java.io.InputStream;
import java.util.Properties;

/** Loads db.* and polygon.* from application.properties with env override (DB_HOST, POLYGON_RPC_URL, etc.). */
public final class Config {
    public final String dbHost;
    public final int dbPort;
    public final String dbName;
    public final String dbUser;
    public final String dbPassword;
    public final String polygonRpcUrl;
    public final String polygonContractAddress;
    public final long polygonStartBlock;
    public final long polygonIngestionIntervalMs;
    public final int polygonIngestionChunkBlocks;
    public final String polymarketGammaApiUrl;
    public final String polymarketSubgraphUrl;
    public final String polymarketWsUrl;
    public final boolean polygonSubgraphBackfillEnabled;
    public final boolean polygonWsLiveEnabled;
    public final int polygonSubgraphPageSize;
    public final long polygonSubgraphBackfillIntervalMs;
    /** Only ingest trades with timestamp >= this (Unix seconds). 0 = no lower bound. Use POLYGON_SUBGRAPH_START_DATE=2026-03-01 for 1 Mar 2026. */
    public final long polygonSubgraphMinTimestamp;
    public final long polymarketMarketsIntervalMs;
    /** Max markets per sync run to re-fetch by slug to refresh is_resolved/resolved_at (0 = disabled). */
    public final int polymarketMarketsResolutionRefreshLimit;

    public Config() {
        Properties p = new Properties();
        try (InputStream in = Config.class.getResourceAsStream("/application.properties")) {
            if (in != null) p.load(in);
        } catch (Exception ignored) { }
        dbHost = orEnv(p.getProperty("db.host"), "DB_HOST", "localhost");
        dbPort = parseInt(orEnv(p.getProperty("db.port"), "DB_PORT", "5432"), 5432);
        dbName = orEnv(p.getProperty("db.database"), "DB_NAME", "tradeflux");
        dbUser = orEnv(p.getProperty("db.user"), "DB_USER", "postgres");
        dbPassword = orEnv(p.getProperty("db.password"), "DB_PASSWORD", "");
        polygonRpcUrl = orEnv(p.getProperty("polygon.rpc.url"), "POLYGON_RPC_URL", "").trim();
        polygonContractAddress = orEnv(p.getProperty("polygon.contract.address"), "POLYGON_CONTRACT_ADDRESS", "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E").trim();
        polygonStartBlock = parseLong(orEnv(p.getProperty("polygon.start.block"), "POLYGON_START_BLOCK", "83700000"), 83_700_000L);
        polygonIngestionIntervalMs = parseLong(orEnv(p.getProperty("polygon.ingestion.intervalMs"), "POLYGON_INGESTION_INTERVAL_MS", "300000"), 300_000L);
        polygonIngestionChunkBlocks = parseInt(orEnv(p.getProperty("polygon.ingestion.chunkBlocks"), "POLYGON_INGESTION_CHUNK_BLOCKS", "2000"), 2000);
        polymarketGammaApiUrl = orEnv(p.getProperty("polymarket.gamma.api.url"), "POLYMARKET_GAMMA_API_URL", "https://gamma-api.polymarket.com").trim();
        polymarketMarketsIntervalMs = parseLong(orEnv(p.getProperty("polymarket.markets.intervalMs"), "POLYMARKET_MARKETS_INTERVAL_MS", "3600000"), 3600_000L);
        polymarketMarketsResolutionRefreshLimit = Math.max(0, parseInt(orEnv(p.getProperty("polymarket.markets.resolutionRefreshLimit"), "POLYMARKET_MARKETS_RESOLUTION_REFRESH_LIMIT", "100"), 100));
        polymarketSubgraphUrl = orEnv(p.getProperty("polymarket.subgraph.url"), "POLYMARKET_SUBGRAPH_URL",
                "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn").trim();
        polymarketWsUrl = orEnv(p.getProperty("polymarket.ws.url"), "POLYMARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market").trim();
        polygonSubgraphBackfillEnabled = Boolean.parseBoolean(orEnv(p.getProperty("polygon.subgraph.backfill.enabled"), "POLYGON_SUBGRAPH_BACKFILL_ENABLED", "true"));
        polygonWsLiveEnabled = Boolean.parseBoolean(orEnv(p.getProperty("polygon.ws.live.enabled"), "POLYGON_WS_LIVE_ENABLED", "true"));
        polygonSubgraphPageSize = parseInt(orEnv(p.getProperty("polygon.subgraph.pageSize"), "POLYGON_SUBGRAPH_PAGE_SIZE", "500"), 500);
        polygonSubgraphBackfillIntervalMs = parseLong(orEnv(p.getProperty("polygon.subgraph.backfill.intervalMs"), "POLYGON_SUBGRAPH_BACKFILL_INTERVAL_MS", "86400000"), 86400_000L);
        polygonSubgraphMinTimestamp = parseSubgraphMinTimestamp(
                orEnv(p.getProperty("polygon.subgraph.startDate"), "POLYGON_SUBGRAPH_START_DATE", ""),
                orEnv(p.getProperty("polygon.subgraph.minTimestamp"), "POLYGON_SUBGRAPH_MIN_TIMESTAMP", "0"));
    }

    private static long parseSubgraphMinTimestamp(String startDate, String minTimestampStr) {
        if (minTimestampStr != null && !minTimestampStr.isEmpty()) {
            try { return Long.parseLong(minTimestampStr.trim()); } catch (NumberFormatException e) { }
        }
        if (startDate != null && !startDate.isEmpty()) {
            try {
                java.time.LocalDate d = java.time.LocalDate.parse(startDate.trim());
                return d.atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond();
            } catch (Exception e) { }
        }
        return 0L;
    }

    private static String orEnv(String fromProps, String envKey, String def) {
        String env = System.getenv(envKey);
        if (env != null && !env.isEmpty()) return env;
        if (fromProps != null && !fromProps.isEmpty() && !fromProps.startsWith("${")) return fromProps;
        return def;
    }

    private static int parseInt(String v, int def) {
        if (v == null || v.isEmpty()) return def;
        try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return def; }
    }

    private static long parseLong(String v, long def) {
        if (v == null || v.isEmpty()) return def;
        try { return Long.parseLong(v.trim()); } catch (NumberFormatException e) { return def; }
    }
}
