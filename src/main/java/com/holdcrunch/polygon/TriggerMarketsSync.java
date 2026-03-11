package com.holdcrunch.polygon;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

/**
 * Runs market sync once and exits. Use same DB_* env as the main app.
 * Example: DB_HOST=... DB_NAME=tradeflux DB_USER=postgres DB_PASSWORD=... \
 *   mvn -q exec:java -Dexec.mainClass="com.holdcrunch.polygon.TriggerMarketsSync"
 */
public class TriggerMarketsSync {
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
                    System.out.println("Markets sync finished (verticle " + id + "). Exiting.");
                    vertx.close();
                })
                .onFailure(err -> {
                    System.err.println("Markets sync failed: " + err.getMessage());
                    vertx.close();
                    System.exit(1);
                });
    }
}
