package com.holdcrunch.polygon;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Entry point for Trade Flux Polygon.
 * <p>Architecture: Polymarket API → polygon_markets; Subgraph (history) + WebSocket (live) → polygon_trades → SQL reports.
 * <p>Startup order: markets first (initial sync), then subgraph backfill + WebSocket.
 * <p>Set DB_* (and optionally POLYGON_SUBGRAPH_START_DATE=2026-03-01). Run db/create_tables.sql once.
 * <p>GET /health returns 200 only when trades are coming through (latest trade within max age); else 503.
 */
public class Application {
    /** If no trade in this many minutes, health is unhealthy. */
    private static final int HEALTH_TRADE_MAX_AGE_MINUTES = 15;

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
        ResolutionRefreshConsumer resolutionConsumer = new ResolutionRefreshConsumer(cfg);

        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "5020"));
        Router router = Router.router(vertx);
        router.get("/health").handler(ctx -> healthWithRecentTrades(ctx, pool));
        router.post("/trigger-markets-sync").handler(ctx -> {
            vertx.eventBus().request(PolymarketMarketsService.ADDRESS_TRIGGER_SYNC, null, ar -> {
                if (ar.succeeded()) ctx.response().setStatusCode(200).end("ok");
                else ctx.response().setStatusCode(500).end(ar.cause().getMessage());
            });
        });
        HttpServer server = vertx.createHttpServer().requestHandler(router);

        server.listen(port)
                .compose(v -> vertx.deployVerticle(markets))
                .compose(mid -> vertx.deployVerticle(ingestion))
                .compose(ingId -> vertx.deployVerticle(resolutionConsumer))
                .onSuccess(id -> System.out.println("Trade Flux Polygon running on port " + port + ". Order: markets → subgraph backfill → WebSocket → resolution consumer."))
                .onFailure(err -> {
                    System.err.println("Deploy failed: " + err.getMessage());
                    vertx.close();
                    System.exit(1);
                });
    }

    /**
     * Healthy only when we have at least one trade and the latest trade is within
     * HEALTH_TRADE_MAX_AGE_MINUTES. Otherwise 503 so Beanstalk can replace the instance.
     */
    private static void healthWithRecentTrades(RoutingContext ctx, PgPool pool) {
        pool.query("SELECT MAX(block_timestamp) AS ts FROM polygon_trades")
                .execute()
                .onFailure(err -> {
                    ctx.response().setStatusCode(503).end("DB error");
                })
                .onSuccess(rs -> {
                    if (rs.iterator().hasNext()) {
                        var row = rs.iterator().next();
                        var ts = row.getOffsetDateTime(0);
                        if (ts != null) {
                            Instant cutoff = Instant.now().minus(HEALTH_TRADE_MAX_AGE_MINUTES, ChronoUnit.MINUTES);
                            if (ts.toInstant().isAfter(cutoff)) {
                                ctx.response().setStatusCode(200).end("OK");
                                return;
                            }
                        }
                    }
                    ctx.response().setStatusCode(503).end("no recent trades");
                });
    }
}
