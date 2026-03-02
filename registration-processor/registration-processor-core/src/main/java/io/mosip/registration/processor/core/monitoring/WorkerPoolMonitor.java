package io.mosip.registration.processor.core.monitoring;

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.vertx.core.Vertx;
import io.vertx.micrometer.backends.BackendRegistries;

/**
 * Utility class for monitoring Vert.x worker pool usage.
 * Uses Vert.x Micrometer metrics for accurate thread tracking.
 * Captures ALL worker thread usage including message processing, health checks, etc.
 */
public class WorkerPoolMonitor {

    private static final Logger logger = RegProcessorLogger.getLogger(WorkerPoolMonitor.class);

    private static Vertx vertxInstance;
    private static String stageName;
    private static long periodicLoggerId = -1;

    private WorkerPoolMonitor() {
    }

    /**
     * Register Vertx instance and stage name for periodic logging.
     */
    public static void registerStage(String name, Vertx vertx) {
        if (vertxInstance == null) {
            vertxInstance = vertx;
            stageName = name;
        }
    }

    /**
     * Start periodic logging of worker pool status.
     * Logs every specified interval - captures ALL thread usage (messages, health checks, etc.)
     *
     * @param intervalSeconds interval between logs in seconds
     */
    public static void startPeriodicLogging(long intervalSeconds) {
        if (vertxInstance == null) {
            logger.warn("WORKER_POOL_MONITOR: Cannot start periodic logging - Vertx instance not registered");
            return;
        }
        if (periodicLoggerId != -1) {
            return;
        }

        periodicLoggerId = vertxInstance.setPeriodic(TimeUnit.SECONDS.toMillis(intervalSeconds), id -> {
            logPoolStatus();
        });
        logger.info("WORKER_POOL_MONITOR: Started periodic logging every {} seconds", intervalSeconds);
    }

    /**
     * Log warning only if requests are queued (pool is saturated).
     * Call this from message processing paths for alert-only logging.
     */
    public static void logIfQueued(String stageName) {
        MeterRegistry registry = BackendRegistries.getDefaultNow();
        if (registry == null) return;

        Double queueSize = getGaugeValue(registry, "vertx.pool.queue.size", "worker");
        if (queueSize != null && queueSize > 0) {
            Double inUse = getGaugeValue(registry, "vertx.pool.inUse", "worker");
            Double poolSize = getGaugeValue(registry, "vertx.pool.size", "worker");
            logger.warn("WORKER_POOL_QUEUE: Stage={}, WorkersInUse={}/{}, QueuedRequests={}",
                    stageName,
                    inUse != null ? inUse.intValue() : "N/A",
                    poolSize != null ? poolSize.intValue() : "N/A",
                    queueSize.intValue());
        }
    }

    /**
     * Log pool status - captures ALL thread usage.
     */
    private static void logPoolStatus() {
        MeterRegistry registry = BackendRegistries.getDefaultNow();
        if (registry == null) {
            return;
        }

        Double inUse = getGaugeValue(registry, "vertx.pool.inUse", "worker");
        Double poolSize = getGaugeValue(registry, "vertx.pool.size", "worker");
        Double queueSize = getGaugeValue(registry, "vertx.pool.queue.size", "worker");
        Double ratio = getGaugeValue(registry, "vertx.pool.ratio", "worker");

        if (inUse != null && poolSize != null) {
            if (queueSize != null && queueSize > 0) {
                logger.warn("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}/{}, QueuedRequests={}, PoolRatio={}",
                        stageName, inUse.intValue(), poolSize.intValue(), queueSize.intValue(),
                        ratio != null ? String.format("%.2f", ratio) : "N/A");
            } else {
                logger.info("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}/{}, QueuedRequests={}, PoolRatio={}",
                        stageName, inUse.intValue(), poolSize.intValue(),
                        queueSize != null ? queueSize.intValue() : 0,
                        ratio != null ? String.format("%.2f", ratio) : "N/A");
            }
        }
    }

    private static Double getGaugeValue(MeterRegistry registry, String metricName, String poolType) {
        Gauge gauge = registry.find(metricName).tag("pool.type", poolType).gauge();
        return gauge != null ? gauge.value() : null;
    }
}
