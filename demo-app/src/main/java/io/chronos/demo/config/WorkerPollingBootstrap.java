package io.chronos.demo.config;

import io.chronos.client.WorkerPollingEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Starts/stops the worker polling engine with the application lifecycle.
 *
 * <p>Polling begins on {@link ApplicationReadyEvent} — after all beans (including
 * any registered {@code TaskExecutor} workers) have been initialized — and stops
 * on {@link ContextClosedEvent}.
 */
@Slf4j
@Component
public class WorkerPollingBootstrap {

    private final WorkerPollingEngine engine;

    public WorkerPollingBootstrap(WorkerPollingEngine engine) {
        this.engine = engine;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startPolling() {
        engine.start();
        log.info("Worker polling engine started");
    }

    @EventListener(ContextClosedEvent.class)
    public void stopPolling() {
        engine.stop();
        log.info("Worker polling engine stopped");
    }
}
