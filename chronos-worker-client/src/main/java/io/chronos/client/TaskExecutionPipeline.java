package io.chronos.client;

import com.google.protobuf.ByteString;
import io.chronos.proto.RecordTaskCompletionRequest;
import io.chronos.proto.RecordTaskCompletionResponse;
import io.chronos.proto.RecordTaskFailureRequest;
import io.chronos.proto.RecordTaskFailureResponse;
import io.chronos.proto.TaskPayload;
import io.chronos.proto.TaskServiceGrpc;
import io.grpc.StatusRuntimeException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * Executes tasks received from the {@link WorkerPollingEngine}.
 *
 * <p>For each task it (1) looks up the {@link TaskExecutor} registered for the
 * task's type, (2) invokes {@code execute}, and (3) reports the outcome back to
 * the server via {@code RecordTaskCompletion} or {@code RecordTaskFailure}.
 */
@Slf4j
public class TaskExecutionPipeline implements TaskHandler {

    private final WorkerRegistry registry;
    private final TaskServiceGrpc.TaskServiceBlockingStub stub;

    public TaskExecutionPipeline(WorkerRegistry registry, GrpcClientChannelFactory channelFactory) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        Objects.requireNonNull(channelFactory, "channelFactory must not be null");
        this.stub = TaskServiceGrpc.newBlockingStub(channelFactory.getChannel());
    }

    @Override
    public void handle(TaskPayload task) {
        String taskType = task.getTaskType();
        TaskExecutor executor = registry.get(taskType);

        if (executor == null) {
            log.warn("No TaskExecutor registered for task type '{}' (task {})",
                taskType, task.getTaskId());
            reportFailure(task, "No executor registered for task type: " + taskType, "");
            return;
        }

        try {
            byte[] result = executor.execute(task);
            reportCompletion(task, result);
        } catch (Exception e) {
            log.error("Task {} (type={}) failed", task.getTaskId(), taskType, e);
            reportFailure(task, messageOf(e), stackTraceOf(e));
        }
    }

    private void reportCompletion(TaskPayload task, byte[] result) {
        try {
            RecordTaskCompletionResponse response = stub.recordTaskCompletion(
                RecordTaskCompletionRequest.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setWorkflowId(task.getWorkflowId())
                    .setResult(ByteString.copyFrom(result == null ? new byte[0] : result))
                    .build());
            log.info("Reported completion of task {} (ack={})",
                task.getTaskId(), response.getAcknowledged());
        } catch (StatusRuntimeException e) {
            log.error("Failed to report completion of task {}: {}",
                task.getTaskId(), e.getStatus(), e);
        }
    }

    private void reportFailure(TaskPayload task, String message, String stackTrace) {
        try {
            RecordTaskFailureResponse response = stub.recordTaskFailure(
                RecordTaskFailureRequest.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setWorkflowId(task.getWorkflowId())
                    .setErrorMessage(message == null ? "" : message)
                    .setStackTrace(stackTrace == null ? "" : stackTrace)
                    .build());
            log.info("Reported failure of task {} (ack={})",
                task.getTaskId(), response.getAcknowledged());
        } catch (StatusRuntimeException e) {
            log.error("Failed to report failure of task {}: {}",
                task.getTaskId(), e.getStatus(), e);
        }
    }

    private String messageOf(Throwable t) {
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    private String stackTraceOf(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
