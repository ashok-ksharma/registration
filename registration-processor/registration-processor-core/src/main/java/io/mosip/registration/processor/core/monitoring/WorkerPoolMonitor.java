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
            stageName = cleanStageName(name);
        }
    }

    /**
     * Remove Spring CGLIB proxy suffix from stage name.
     */
    private static String cleanStageName(String name) {
        if (name != null && name.contains("$$")) {
            return name.substring(0, name.indexOf("$$"));
        }
        return name;
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
            try {
                logPoolStatus();
            } catch (Exception e) {
                logger.error("WORKER_POOL_MONITOR: Error in periodic logging", e);
            }
        });
        logger.warn("WORKER_POOL_MONITOR: Started periodic logging every {} seconds for stage {}", intervalSeconds, stageName);
    }

    /**
     * Log warning only if requests are queued (pool is saturated).
     * Call this from message processing paths for alert-only logging.
     */
    public static void logIfQueued(String stageNameParam) {
        MeterRegistry registry = BackendRegistries.getDefaultNow();
        if (registry == null) return;

        Double queueSize = getGaugeValue(registry, "vertx.pool.queue.size", "worker");
        if (queueSize != null && queueSize > 0) {
            Double inUse = getGaugeValue(registry, "vertx.pool.inUse", "worker");
            String cleanName = cleanStageName(stageNameParam);
            logger.warn("WORKER_POOL_QUEUE: Stage={}, WorkersInUse={}, QueuedRequests={}",
                    cleanName,
                    inUse != null ? inUse.intValue() : "N/A",
                    queueSize.intValue());
        }
    }

    /**
     * Log pool status - captures ALL thread usage.
     */
    private static void logPoolStatus() {
        MeterRegistry registry = BackendRegistries.getDefaultNow();
        if (registry == null) {
            logger.warn("WORKER_POOL_STATUS: Stage={}, Micrometer registry not available", stageName);
            return;
        }

        Double inUse = getGaugeValue(registry, "vertx.pool.inUse", "worker");
        Double queueSize = getGaugeValue(registry, "vertx.pool.queue.size", "worker");
        Double ratio = getGaugeValue(registry, "vertx.pool.ratio", "worker");

        if (inUse == null) {
            logger.warn("WORKER_POOL_STATUS: Stage={}, Metrics not available (inUse=null)", stageName);
            return;
        }

        int inUseInt = inUse.intValue();
        int queueInt = queueSize != null ? queueSize.intValue() : 0;
        String ratioStr = ratio != null ? String.format("%.2f", ratio) : "N/A";

        if (queueInt > 0) {
            logger.warn("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}, QueuedRequests={}, PoolRatio={}",
                    stageName, inUseInt, queueInt, ratioStr);
        } else {
            logger.warn("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}, QueuedRequests={}, PoolRatio={}",
                    stageName, inUseInt, queueInt, ratioStr);
        }
    }

    private static Double getGaugeValue(MeterRegistry registry, String metricName, String poolType) {
        Gauge gauge = registry.find(metricName).tag("pool.type", poolType).gauge();
        logger.debug("WORKER_POOL_GAUGE: metricName={}, tag=pool.type={}, gauge={}", metricName, poolType, gauge);
        
        if (gauge == null) {
            gauge = registry.find(metricName.replace(".", "_")).tag("pool_type", poolType).gauge();
            logger.debug("WORKER_POOL_GAUGE: metricName={}, tag=pool_type={}, gauge={}", 
                    metricName.replace(".", "_"), poolType, gauge);
        }
        if (gauge == null) {
            gauge = registry.find(metricName).tag("pool_type", poolType).gauge();
            logger.debug("WORKER_POOL_GAUGE: metricName={}, tag=pool_type={}, gauge={}", metricName, poolType, gauge);
        }
        
        Double value = gauge != null ? gauge.value() : null;
        logger.warn("WORKER_POOL_GAUGE: metricName={}, value={}", metricName, value);
        return value;
    }
}
