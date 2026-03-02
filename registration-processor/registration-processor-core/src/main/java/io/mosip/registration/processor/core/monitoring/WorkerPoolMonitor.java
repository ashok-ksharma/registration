package io.mosip.registration.processor.core.monitoring;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;

/**
 * Utility class for monitoring Vert.x worker pool usage.
 * Tracks active worker threads and queued requests per stage.
 */
public class WorkerPoolMonitor {

    private static final Logger logger = RegProcessorLogger.getLogger(WorkerPoolMonitor.class);

    private static final Map<String, StageWorkerMetrics> stageMetrics = new ConcurrentHashMap<>();

    private WorkerPoolMonitor() {
    }

    /**
     * Register a stage with its configured worker pool size.
     */
    public static void registerStage(String stageName, int workerPoolSize) {
        stageMetrics.put(stageName, new StageWorkerMetrics(workerPoolSize));
    }

    /**
     * Called when a request arrives (before worker thread is assigned).
     * If all workers are busy, request is queued.
     * 
     * @return true if request was queued (pool was full), false otherwise
     */
    public static boolean requestArrived(String stageName) {
        StageWorkerMetrics metrics = stageMetrics.get(stageName);
        if (metrics == null) return false;

        int activeWorkers = metrics.getActiveCount();
        int poolSize = metrics.getWorkerPoolSize();

        if (activeWorkers >= poolSize) {
            int queueDepth = metrics.incrementQueuedCount();
            metrics.incrementTotalQueuedCount();
            logger.warn("WORKER_POOL_QUEUE: Stage={}, WorkersInUse={}/{}, QueuedRequests={}",
                    stageName, activeWorkers, poolSize, queueDepth);
            return true;
        }
        return false;
    }

    /**
     * Called when a worker thread starts processing.
     * 
     * @param wasQueued true if this request was queued when it arrived
     */
    public static void threadAcquired(String stageName, boolean wasQueued) {
        StageWorkerMetrics metrics = stageMetrics.get(stageName);
        if (metrics == null) return;

        if (wasQueued) {
            metrics.decrementQueuedCount();
        }

        int activeWorkers = metrics.incrementActiveCount();
        int poolSize = metrics.getWorkerPoolSize();
        metrics.updatePeakIfHigher(activeWorkers);

        logger.info("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}/{}, QueuedRequests={}",
                stageName, activeWorkers, poolSize, metrics.getQueuedCount());
    }

    /**
     * Called when a worker thread finishes processing.
     */
    public static void threadReleased(String stageName) {
        StageWorkerMetrics metrics = stageMetrics.get(stageName);
        if (metrics == null) return;

        metrics.incrementTotalProcessed();
        int activeWorkers = metrics.decrementActiveCount();
        int poolSize = metrics.getWorkerPoolSize();

        logger.info("WORKER_POOL_STATUS: Stage={}, WorkersInUse={}/{}, QueuedRequests={}",
                stageName, activeWorkers, poolSize, metrics.getQueuedCount());
    }

    /**
     * Get current snapshot of worker pool status for a stage.
     */
    public static String getPoolStatus(String stageName) {
        StageWorkerMetrics metrics = stageMetrics.get(stageName);
        if (metrics == null) {
            return "Stage " + stageName + " not registered";
        }
        return String.format("Stage=%s, WorkersInUse=%d/%d, QueuedRequests=%d, PeakWorkers=%d, TotalProcessed=%d, TotalQueued=%d",
                stageName, metrics.getActiveCount(), metrics.getWorkerPoolSize(),
                metrics.getQueuedCount(), metrics.getPeakActiveCount(),
                metrics.getTotalProcessed(), metrics.getTotalQueuedCount());
    }

    /**
     * Log current pool status for all stages.
     */
    public static void logAllPoolStatus() {
        logger.info("=== WORKER_POOL_STATUS_SNAPSHOT ===");
        stageMetrics.forEach((stageName, metrics) -> {
            logger.info("WORKER_POOL_SNAPSHOT: Stage={}, WorkersInUse={}/{}, QueuedRequests={}, PeakWorkers={}, TotalProcessed={}, TotalQueued={}",
                    stageName, metrics.getActiveCount(), metrics.getWorkerPoolSize(),
                    metrics.getQueuedCount(), metrics.getPeakActiveCount(),
                    metrics.getTotalProcessed(), metrics.getTotalQueuedCount());
        });
        logger.info("=== END WORKER_POOL_STATUS_SNAPSHOT ===");
    }

    /**
     * Inner class to hold metrics for each stage.
     */
    private static class StageWorkerMetrics {
        private final int workerPoolSize;
        private final AtomicInteger activeCount = new AtomicInteger(0);
        private final AtomicInteger queuedCount = new AtomicInteger(0);
        private final AtomicInteger peakActiveCount = new AtomicInteger(0);
        private final AtomicLong totalProcessed = new AtomicLong(0);
        private final AtomicLong totalQueuedCount = new AtomicLong(0);

        StageWorkerMetrics(int workerPoolSize) {
            this.workerPoolSize = workerPoolSize;
        }

        int getWorkerPoolSize() {
            return workerPoolSize;
        }

        int getActiveCount() {
            return activeCount.get();
        }

        int getQueuedCount() {
            return queuedCount.get();
        }

        int getPeakActiveCount() {
            return peakActiveCount.get();
        }

        long getTotalProcessed() {
            return totalProcessed.get();
        }

        long getTotalQueuedCount() {
            return totalQueuedCount.get();
        }

        int incrementActiveCount() {
            return activeCount.incrementAndGet();
        }

        int decrementActiveCount() {
            return activeCount.decrementAndGet();
        }

        int incrementQueuedCount() {
            return queuedCount.incrementAndGet();
        }

        int decrementQueuedCount() {
            return queuedCount.decrementAndGet();
        }

        void incrementTotalQueuedCount() {
            totalQueuedCount.incrementAndGet();
        }

        void incrementTotalProcessed() {
            totalProcessed.incrementAndGet();
        }

        void updatePeakIfHigher(int currentActive) {
            peakActiveCount.updateAndGet(peak -> Math.max(peak, currentActive));
        }
    }
}
