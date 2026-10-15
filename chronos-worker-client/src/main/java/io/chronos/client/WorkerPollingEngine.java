package io.chronos.client;

import io.chronos.proto.PollTaskQueueRequest;
import io.chronos.proto.PollTaskQueueResponse;
import io.chronos.proto.TaskPayload;
import io.chronos.proto.TaskServiceGrpc;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

/**
 * Continuously polls the server's {@code pollTaskQueue} endpoint and hands each
 * received task to a {@link TaskHandler}.
 *
 * <p>Polling runs on Java 21 virtual threads
 * ({@link Executors#newVirtualThreadPerTaskExecutor()}). Because the server's
 * poll is a long-poll, each poller simply blocks on the RPC for up to
 * {@code pollTimeoutMs} and re-polls immediately; many concurrent pollers cost
 * almost nothing since blocked virtual threads are unmounted from carrier
 * threads.
 */
@Slf4j
public class WorkerPollingEngine implements AutoCloseable {

    private static final int DEFAULT_POLLER_COUNT = 2;
    private static final long DEFAULT_POLL_TIMEOUT_MS = 5_000L;
    private static final long DEFAULT_BACKOFF_MS = 1_000L;

    private final GrpcClientChannelFactory channelFactory;
    private final TaskHandler taskHandler;
    private final List<String> taskTypes = new ArrayList<>();

    private int pollerCount = DEFAULT_POLLER_COUNT;
    private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
    private long backoffMs = DEFAULT_BACKOFF_MS;

    private volatile boolean running = false;
    private volatile TaskServiceGrpc.TaskServiceBlockingStub stub;
    private ExecutorService pollerExecutor;

    public WorkerPollingEngine(GrpcClientChannelFactory channelFactory, TaskHandler taskHandler) {
        this.channelFactory = Objects.requireNonNull(channelFactory, "channelFactory must not be null");
        this.taskHandler = Objects.requireNonNull(taskHandler, "taskHandler must not be null");
    }

    /** Sets the number of concurrent poller virtual threads. Defaults to 2. */
    public WorkerPollingEngine withPollerCount(int pollerCount) {
        if (pollerCount <= 0) {
            throw new IllegalArgumentException("pollerCount must be greater than zero");
        }
        this.pollerCount = pollerCount;
        return this;
    }

    /** Sets the server-side long-poll timeout in milliseconds. */
    public WorkerPollingEngine withPollTimeoutMs(long pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
        return this;
    }

    /** Sets the backoff (ms) between retries after a poll failure. */
    public WorkerPollingEngine withBackoffMs(long backoffMs) {
        this.backoffMs = backoffMs;
        return this;
    }

    /** Restricts polling to the given task types; empty means "any type". */
    public WorkerPollingEngine withTaskTypes(String... taskTypes) {
        this.taskTypes.addAll(Arrays.asList(taskTypes));
        return this;
    }

    /**
     * Starts the polling loop. Creates the shared gRPC stub once, then spawns
     * {@code pollerCount} virtual-thread pollers.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        this.stub = TaskServiceGrpc.newBlockingStub(channelFactory.getChannel());
        this.pollerExecutor = Executors.newVirtualThreadPerTaskExecutor();

        for (int i = 0; i < pollerCount; i++) {
            pollerExecutor.submit(this::pollLoop);
        }
        log.info("Started {} poller(s) for task types {}",
            pollerCount, taskTypes.isEmpty() ? "[any]" : taskTypes);
    }

    /**
     * Stops polling: interrupts pollers and closes the underlying channel,
     * which cancels any in-flight long-poll RPCs.
     */
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (pollerExecutor != null) {
            pollerExecutor.shutdownNow();
            pollerExecutor = null;
        }
        channelFactory.close();
        log.info("Stopped polling engine");
    }

    private void pollLoop() {
        log.info("Poller started on thread {}", Thread.currentThread().getName());
        while (running) {
            try {
                PollTaskQueueResponse response = stub.pollTaskQueue(
                    PollTaskQueueRequest.newBuilder()
                        .addAllTaskTypes(taskTypes)
                        .setPollTimeoutMs(pollTimeoutMs)
                        .build());

                if (response.getHasTask()) {
                    TaskPayload task = response.getPayload();
                    log.info("Received task {} (type={})", task.getTaskId(), task.getTaskType());
                    taskHandler.handle(task);
                }
            } catch (StatusRuntimeException e) {
                if (!running) {
                    break;
                }
                log.warn("pollTaskQueue failed ({}); backing off {}ms", e.getStatus(), backoffMs);
                sleepQuietly(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in polling loop", e);
            }
        }
        log.info("Poller stopped on thread {}", Thread.currentThread().getName());
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        stop();
    }
}
