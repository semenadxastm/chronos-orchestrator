package io.chronos.server.grpc;

import io.chronos.proto.PollTaskQueueRequest;
import io.chronos.proto.PollTaskQueueResponse;
import io.chronos.proto.TaskPayload;
import io.chronos.proto.TaskServiceGrpc;
import io.chronos.server.messaging.TaskQueue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * gRPC implementation of {@link TaskServiceGrpc.TaskServiceImplBase}.
 *
 * <p>Workers call {@link #pollTaskQueue} to long-poll for the next task. The
 * implementation blocks on the in-memory {@link TaskQueue} (populated by the
 * Kafka consumer) for up to {@code poll_timeout_ms}, returning either a single
 * task or an empty response. Because the handler is executed on a virtual
 * thread, blocking on the queue costs no OS thread.
 */
@Slf4j
@GrpcService
public class TaskServiceImpl extends TaskServiceGrpc.TaskServiceImplBase {

    private static final long DEFAULT_POLL_TIMEOUT_MS = 5_000L;

    private final TaskQueue taskQueue;

    public TaskServiceImpl(TaskQueue taskQueue) {
        this.taskQueue = taskQueue;
    }

    @Override
    public void pollTaskQueue(
        PollTaskQueueRequest request,
        StreamObserver<PollTaskQueueResponse> responseObserver
    ) {
        long timeoutMs = request.getPollTimeoutMs() > 0
            ? request.getPollTimeoutMs()
            : DEFAULT_POLL_TIMEOUT_MS;

        // Empty task type list means "any task".
        Set<String> acceptedTypes = request.getTaskTypesList().isEmpty()
            ? null
            : new HashSet<>(request.getTaskTypesList());

        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);

        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    respondEmpty(responseObserver);
                    return;
                }

                TaskPayload task = taskQueue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                if (task == null) {
                    respondEmpty(responseObserver);
                    return;
                }

                if (acceptedTypes == null || acceptedTypes.contains(task.getTaskType())) {
                    log.info("Dispatching task {} (type={}) to worker",
                        task.getTaskId(), task.getTaskType());
                    responseObserver.onNext(PollTaskQueueResponse.newBuilder()
                        .setHasTask(true)
                        .setPayload(task)
                        .build());
                    responseObserver.onCompleted();
                    return;
                }

                // Not a type this worker is asking for; put it back so a
                // different worker can pick it up.
                taskQueue.enqueue(task);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Task polling was interrupted", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Task polling was interrupted")
                .withCause(e)
                .asRuntimeException());
        }
    }

    private void respondEmpty(StreamObserver<PollTaskQueueResponse> responseObserver) {
        responseObserver.onNext(PollTaskQueueResponse.newBuilder()
            .setHasTask(false)
            .build());
        responseObserver.onCompleted();
    }
}
