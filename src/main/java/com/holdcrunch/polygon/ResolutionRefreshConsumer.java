package com.holdcrunch.polygon;

import com.rabbitmq.client.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes slugs from RabbitMQ resolution-refresh queue, fetches Gamma by slug and upserts.
 * Delegates to PolymarketMarketsService via event bus.
 * Rate-limits Gamma API calls; uses prefetch for higher throughput.
 */
public class ResolutionRefreshConsumer extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ResolutionRefreshConsumer.class);

    private final Config cfg;
    private Connection connection;
    private Channel channel;
    private String consumerTag;
    private Long rateLimitTimerId;
    private final AtomicInteger gammaPermits = new AtomicInteger(0);

    public ResolutionRefreshConsumer(Config cfg) {
        this.cfg = cfg;
    }

    @Override
    public void start(Promise<Void> p) {
        if (cfg.rabbitHost == null || cfg.rabbitHost.isEmpty()) {
            log.info("RabbitMQ host not set; resolution refresh consumer disabled");
            p.complete();
            return;
        }
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(cfg.rabbitHost);
            factory.setPort(cfg.rabbitPort);
            factory.setUsername(cfg.rabbitUser);
            factory.setPassword(cfg.rabbitPassword);
            factory.setVirtualHost(cfg.rabbitVhost);
            try {
                factory.useSslProtocol();
            } catch (Exception e) {
                log.warn("RabbitMQ SSL config failed: {}", e.getMessage());
            }
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(cfg.rabbitQueueResolutionRefresh, true, false, false, null);
            channel.basicQos(cfg.polygonResolutionConsumerPrefetch);

            // Refill permits every second (rate limit Gamma calls)
            gammaPermits.set(cfg.polygonResolutionGammaMaxCallsPerSecond);
            rateLimitTimerId = vertx.setPeriodic(1000, id -> gammaPermits.set(cfg.polygonResolutionGammaMaxCallsPerSecond));

            DeliverCallback deliverCallback = (tag, delivery) -> {
                String slug = new String(delivery.getBody(), StandardCharsets.UTF_8).trim();
                if (slug.isEmpty()) {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    return;
                }
                processWhenReady(delivery, slug);
            };

            consumerTag = channel.basicConsume(cfg.rabbitQueueResolutionRefresh, false, deliverCallback, t -> {});
            log.info("Resolution refresh consumer started on queue {}, prefetch={}, gammaMaxCallsPerSecond={}",
                    cfg.rabbitQueueResolutionRefresh, cfg.polygonResolutionConsumerPrefetch, cfg.polygonResolutionGammaMaxCallsPerSecond);
            p.complete();
        } catch (IOException | TimeoutException e) {
            log.error("Failed to start resolution refresh consumer: {}", e.getMessage());
            p.fail(e);
        }
    }

    /** Acquire rate-limit permit, then call Gamma. Reschedules if no permit. */
    private void processWhenReady(Delivery delivery, String slug) {
        int p = gammaPermits.getAndDecrement();
        if (p > 0) {
            doRefresh(delivery, slug);
        } else {
            gammaPermits.incrementAndGet();
            long delayMs = Math.max(20, 1000 / cfg.polygonResolutionGammaMaxCallsPerSecond);
            vertx.setTimer(delayMs, id -> processWhenReady(delivery, slug));
        }
    }

    private void doRefresh(Delivery delivery, String slug) {
        vertx.eventBus().<Integer>request(PolymarketMarketsService.ADDRESS_REFRESH_BY_SLUG, slug, ar -> {
            if (ar.succeeded()) {
                try {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (IOException e) {
                    log.warn("Failed to ack: {}", e.getMessage());
                }
            } else {
                try {
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                } catch (IOException e) {
                    log.warn("Failed to nack: {}", e.getMessage());
                }
            }
        });
    }

    @Override
    public void stop(Promise<Void> p) {
        if (rateLimitTimerId != null) {
            vertx.cancelTimer(rateLimitTimerId);
        }
        try {
            if (channel != null && consumerTag != null) {
                channel.basicCancel(consumerTag);
            }
            if (channel != null) channel.close();
            if (connection != null) connection.close();
        } catch (IOException | TimeoutException e) {
            log.warn("Error closing Rabbit connection: {}", e.getMessage());
        }
        p.complete();
    }
}
