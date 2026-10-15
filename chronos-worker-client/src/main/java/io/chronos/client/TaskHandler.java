package io.chronos.client;

import io.chronos.proto.TaskPayload;

/**
 * Callback invoked by the {@link WorkerPollingEngine} for each task received
 * from the server. Implementations route the task to a {@link TaskExecutor},
 * execute it, and report the result back to the server.
 */
@FunctionalInterface
public interface TaskHandler {

    /**
     * Handles a single received task.
     *
     * @param task the task to process
     * @throws Exception if handling fails unexpectedly
     */
    void handle(TaskPayload task) throws Exception;
}
