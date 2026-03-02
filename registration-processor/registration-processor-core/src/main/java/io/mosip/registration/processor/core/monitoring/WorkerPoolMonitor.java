package io.mosip.registration.processor.core.monitoring;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.micrometer.backends.BackendRegistries;

/**
 * Utility class for monitoring Vert.x worker pool usage per stage.
 * Uses named WorkerExecutor per stage to get per-stage metrics from Vert.x Micrometer.
 */
public class WorkerPoolMonitor {

    private static final Logger logger = RegProcessorLogger.getLogger(WorkerPoolMonitor.class);

    private static final Map<String, StageInfo> stageInfoMap = new ConcurrentHashMap<>();
    private static Vertx primaryVertx;
    private static long periodicTimerId = -1;

    private WorkerPoolMonitor() {
    }

    /**
     * Register a stage and create a named WorkerExecutor for it.
     * Named pools allow Vert.x Micrometer to track metrics per stage.
     */
    public static WorkerExecutor registerStage(String name, int poolSize, Vertx vertx) {
        String cleanName = cleanStageName(name);
        String poolName = "stage-pool-" + cleanName;
        
        WorkerExecutor executor = vertx.createSharedWorkerExecutor(poolName, poolSize);
        stageInfoMap.put(cleanName, new StageInfo(poolName, poolSize, executor));
        
        if (primaryVertx == null) {
            primaryVertx = vertx;
        }
        
        logger.info("WORKER_POOL_MONITOR: Registered stage={} with poolName={}, poolSize={}", 
                cleanName, poolName, poolSize);
        return executor;
    }

    /**
     * Get the WorkerExecutor for a stage.
     */
    public static WorkerExecutor getWorkerExecutor(String stageName) {
        String cleanName = cleanStageName(stageName);
        StageInfo info = stageInfoMap.get(cleanName);
        return info != null ? info.executor : null;
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
     * Start periodic logging of worker pool status for all stages.
     */
    public static void startPeriodicLogging(long intervalSeconds) {
        if (primaryVertx == null) {
            logger.warn("WORKER_POOL_MONITOR: Cannot start periodic logging - no Vertx instance registered");
            return;
        }
        if (periodicTimerId != -1) {
            return;
        }

        periodicTimerId = primaryVertx.setPeriodic(TimeUnit.SECONDS.toMillis(intervalSeconds), id -> {
            try {
                logAllStagesStatus();
            } catch (Exception e) {
                logger.error("WORKER_POOL_MONITOR: Error in periodic logging", e);
            }
        });
        logger.info("WORKER_POOL_MONITOR: Started periodic logging every {} seconds", intervalSeconds);
    }

    /**
     * Log pool status for each registered stage using Vert.x Micrometer metrics.
     */
    private static void logAllStagesStatus() {
        MeterRegistry registry = BackendRegistries.getDefaultNow();
        if (registry == null) {
            logger.warn("WORKER_POOL_MONITOR: MeterRegistry not available");
            return;
        }

        for (Map.Entry<String, StageInfo> entry : stageInfoMap.entrySet()) {
            String stageName = entry.getKey();
            StageInfo info = entry.getValue();
            
            double inUse = getMetricValue(registry, "vertx.pool.inUse", info.poolName);
            double queueSize = getMetricValue(registry, "vertx.pool.queue.size", info.poolName);
            double ratio = getMetricValue(registry, "vertx.pool.ratio", info.poolName);
            
            int poolSize = info.poolSize;
            int workersInUse = (int) inUse;
            int queued = (int) queueSize;

            if (queued > 0) {
                logger.warn("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}/{}, QueuedRequests={}, PoolRatio={}",
                        stageName, workersInUse, poolSize, queued, String.format("%.2f", ratio));
            } else {
                logger.info("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}/{}, QueuedRequests={}, PoolRatio={}",
                        stageName, workersInUse, poolSize, queued, String.format("%.2f", ratio));
            }
        }
    }

    /**
     * Get metric value for a specific pool name.
     */
    private static double getMetricValue(MeterRegistry registry, String metricName, String poolName) {
        Gauge gauge = registry.find(metricName)
                .tag("pool.type", "worker")
                .tag("pool.name", poolName)
                .gauge();
        return gauge != null ? gauge.value() : 0.0;
    }

    /**
     * Info about a stage's worker pool.
     */
    private static class StageInfo {
        final String poolName;
        final int poolSize;
        final WorkerExecutor executor;

        StageInfo(String poolName, int poolSize, WorkerExecutor executor) {
            this.poolName = poolName;
            this.poolSize = poolSize;
            this.executor = executor;
        }
    }
}
