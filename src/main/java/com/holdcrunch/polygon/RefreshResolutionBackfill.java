package com.holdcrunch.polygon;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

/**
 * One-off: run resolution refresh repeatedly until few or no unresolved markets remain.
 * Use same DB_* env as the main app. Set POLYMARKET_MARKETS_RESOLUTION_REFRESH_LIMIT=100 (default).
 * Example: DB_HOST=... DB_PASSWORD=... mvn -q exec:java -Dexec.mainClass="com.holdcrunch.polygon.RefreshResolutionBackfill"
 */
public class RefreshResolutionBackfill {
    private static final int MAX_ROUNDS = 50;

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        Config cfg = new Config();
        PgConnectOptions pgOpts = new PgConnectOptions()
                .setHost(cfg.dbHost)
                .setPort(cfg.dbPort)
                .setDatabase(cfg.dbName)
                .setUser(cfg.dbUser)
                .setPassword(cfg.dbPassword);
        PgPool pool = PgPool.pool(vertx, pgOpts, new PoolOptions().setMaxSize(5));
        WebClient client = WebClient.create(vertx);
        PolymarketMarketsService markets = new PolymarketMarketsService(cfg, client, pool);
        vertx.deployVerticle(markets)
                .onSuccess(id -> {
                    // Let initial sync (from start()) finish, then run manual sync in a loop
                    vertx.setTimer(10_000, tid -> triggerLoop(vertx, id, 0));
                })
                .onFailure(err -> {
                    System.err.println("Deploy failed: " + err.getMessage());
                    vertx.close();
                    System.exit(1);
                });
    }

    private static void triggerLoop(Vertx vertx, String deploymentId, int round) {
        if (round >= MAX_ROUNDS) {
            System.out.println("Reached max rounds " + MAX_ROUNDS + ". Exiting.");
            vertx.undeploy(deploymentId).onComplete(ar -> vertx.close());
            return;
        }
        vertx.eventBus().request(PolymarketMarketsService.ADDRESS_TRIGGER_SYNC, null, ar -> {
            if (ar.failed()) {
                System.err.println("Round " + (round + 1) + " failed: " + ar.cause().getMessage());
            } else {
                System.out.println("Round " + (round + 1) + " completed.");
            }
            vertx.setTimer(2_000, tid -> triggerLoop(vertx, deploymentId, round + 1));
        });
    }
}
