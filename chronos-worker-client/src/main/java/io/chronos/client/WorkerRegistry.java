package io.chronos.client;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Registry mapping task types to their {@link TaskExecutor} implementations.
 *
 * <p>Users register one executor per task type (e.g. {@code "PROCESS_PAYMENT"}).
 * The polling engine looks up the executor by the {@code task_type} carried on
 * each incoming {@link io.chronos.proto.TaskPayload} and dispatches the task to
 * it.
 *
 * <p>This class is thread-safe: executors are stored in a
 * {@link ConcurrentHashMap} and may be registered/looked up from multiple
 * polling threads concurrently.
 */
@Slf4j
public class WorkerRegistry {

    private final Map<String, TaskExecutor> executors = new ConcurrentHashMap<>();

    /**
     * Registers an executor for a task type, replacing any previously
     * registered executor for the same type.
     *
     * @param taskType the task type (must not be null)
     * @param executor the executor (must not be null)
     */
    public void register(String taskType, TaskExecutor executor) {
        String type = Objects.requireNonNull(taskType, "taskType must not be null");
        TaskExecutor registered = Objects.requireNonNull(executor, "executor must not be null");

        TaskExecutor previous = executors.put(type, registered);
        if (previous != null) {
            log.warn("Replacing existing TaskExecutor for task type '{}'", type);
        } else {
            log.info("Registered TaskExecutor for task type '{}'", type);
        }
    }

    /**
     * Returns the executor registered for the given task type, or {@code null}
     * if none is registered.
     */
    public TaskExecutor get(String taskType) {
        return executors.get(taskType);
    }

    /**
     * Returns {@code true} when an executor is registered for the given task type.
     */
    public boolean supports(String taskType) {
        return taskType != null && executors.containsKey(taskType);
    }

    /**
     * Number of registered executors.
     */
    public int size() {
        return executors.size();
    }
}
