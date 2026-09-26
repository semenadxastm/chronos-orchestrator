package io.chronos.server.messaging;

import io.chronos.proto.TaskPayload;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * In-memory, thread-safe hand-off queue of scheduled tasks.
 *
 * <p>The {@link KafkaTaskConsumer} enqueues tasks as they arrive from Kafka,
 * and the gRPC {@code pollTaskQueue} endpoint dequeues them for workers. A
 * {@link LinkedBlockingQueue} is used so that a blocked {@code poll} parks the
 * (virtual) thread rather than consuming an OS thread.
 */
@Component
public class TaskQueue {

    private final BlockingQueue<TaskPayload> queue = new LinkedBlockingQueue<>();

    /**
     * Enqueues a task for later dispatch. Never blocks; the underlying queue is
     * unbounded.
     *
     * @param task the task to enqueue
     * @return {@code true} if the task was accepted
     */
    public boolean enqueue(TaskPayload task) {
        return queue.offer(task);
    }

    /**
     * Waits up to the given timeout for a task to become available.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of {@code timeout}
     * @return the next task, or {@code null} if the timeout elapsed
     * @throws InterruptedException if interrupted while waiting
     */
    public TaskPayload poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    /** Number of tasks currently buffered. */
    public int size() {
        return queue.size();
    }
}
