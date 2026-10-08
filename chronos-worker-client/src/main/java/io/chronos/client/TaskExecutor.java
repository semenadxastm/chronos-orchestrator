package io.chronos.client;

import io.chronos.proto.TaskPayload;

/**
 * User-implemented business logic for a specific task type.
 *
 * <p>End users implement this interface to encode their domain logic (e.g.
 * charge a card, reserve inventory). Implementations are registered against a
 * task type in the worker registry and invoked by the worker's polling engine
 * whenever a matching task is received.
 *
 * <p>The returned {@code byte[]} is reported back to the server via
 * {@code RecordTaskCompletion}; throwing any exception reports the task as
 * failed via {@code RecordTaskFailure}.
 */
@FunctionalInterface
public interface TaskExecutor {

    /**
     * Executes the business logic for a task.
     *
     * @param payload the task to execute (carries input bytes and metadata)
     * @return the serialized result of the task
     * @throws Exception when the task fails
     */
    byte[] execute(TaskPayload payload) throws Exception;
}
