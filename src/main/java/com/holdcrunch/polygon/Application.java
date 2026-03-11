package com.holdcrunch.polygon;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

/**
 * Entry point for Trade Flux Polygon.
 * <p>Architecture: Polymarket API → polygon_markets; Subgraph (history) + WebSocket (live) → polygon_trades → SQL reports.
 * <p>Startup order: markets first (initial sync), then subgraph backfill + WebSocket.
 * <p>Set DB_* (and optionally POLYGON_SUBGRAPH_START_DATE=2026-03-01). Run db/create_tables.sql once.
 */
public class Application {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        Config cfg = new Config();
        PgConnectOptions pgOpts = new PgConnectOptions()
                .setHost(cfg.dbHost)
                .setPort(cfg.dbPort)
                .setDatabase(cfg.dbName)
                .setUser(cfg.dbUser)
                .setPassword(cfg.dbPassword);
        PgPool pool = PgPool.pool(vertx, pgOpts, new PoolOptions().setMaxSize(10));
        WebClient client = WebClient.create(vertx);
        PolymarketMarketsService markets = new PolymarketMarketsService(cfg, client, pool);
        PolygonIngestionService ingestion = new PolygonIngestionService(cfg, client, pool);
        vertx.deployVerticle(markets)
                .compose(mid -> vertx.deployVerticle(ingestion))
                .onSuccess(ingId -> System.out.println("Trade Flux Polygon running. Order: markets → subgraph backfill → WebSocket; both saved to PostgreSQL."))
                .onFailure(err -> {
                    System.err.println("Deploy failed: " + err.getMessage());
                    vertx.close();
                    System.exit(1);
                });
    }
}
