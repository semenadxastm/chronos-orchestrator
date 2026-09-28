package io.chronos.server.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.chronos.proto.HistoryEvent;
import io.chronos.proto.TaskCompleted;
import io.chronos.proto.TaskFailed;
import io.chronos.server.domain.EventType;
import io.chronos.server.domain.WorkflowState;
import io.chronos.server.entity.EventHistoryEntity;
import io.chronos.server.entity.WorkflowExecutionEntity;
import io.chronos.server.engine.WorkflowStateMachine;
import io.chronos.server.exception.WorkflowNotFoundException;
import io.chronos.server.repository.EventHistoryRepository;
import io.chronos.server.repository.WorkflowExecutionRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Coordinates mutations to the event store and the derived workflow lifecycle.
 *
 * <p>When a worker reports a task result, this service appends the
 * corresponding event to the workflow's append-only log and then replays the
 * history through the {@link WorkflowStateMachine} to determine whether the
 * workflow has reached a terminal state.
 */
@Slf4j
@Service
public class WorkflowService {

    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

    private final EventHistoryRepository eventHistoryRepository;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final WorkflowStateMachine workflowStateMachine;

    public WorkflowService(
        EventHistoryRepository eventHistoryRepository,
        WorkflowExecutionRepository workflowExecutionRepository,
        WorkflowStateMachine workflowStateMachine
    ) {
        this.eventHistoryRepository = eventHistoryRepository;
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.workflowStateMachine = workflowStateMachine;
    }

    /**
     * Appends a {@code TASK_COMPLETED} event and re-evaluates workflow state.
     */
    @Transactional
    public void recordTaskCompletion(String workflowId, String taskId, ByteString result) {
        ensureWorkflowExists(workflowId);

        HistoryEvent event = HistoryEvent.newBuilder()
            .setTaskCompleted(TaskCompleted.newBuilder()
                .setTaskId(taskId)
                .setResult(result))
            .build();

        persistEvent(workflowId, EventType.TASK_COMPLETED, event);
        evaluate(workflowId);
    }

    /**
     * Appends a {@code TASK_FAILED} event and re-evaluates workflow state.
     */
    @Transactional
    public void recordTaskFailure(
        String workflowId,
        String taskId,
        String errorMessage,
        String stackTrace
    ) {
        ensureWorkflowExists(workflowId);

        HistoryEvent event = HistoryEvent.newBuilder()
            .setTaskFailed(TaskFailed.newBuilder()
                .setTaskId(taskId)
                .setErrorMessage(errorMessage == null ? "" : errorMessage)
                .setStackTrace(stackTrace == null ? "" : stackTrace))
            .build();

        persistEvent(workflowId, EventType.TASK_FAILED, event);
        evaluate(workflowId);
    }

    private void persistEvent(String workflowId, EventType eventType, HistoryEvent event) {
        long nextSequenceId = eventHistoryRepository.findMaxSequenceIdByWorkflowId(workflowId) + 1;
        Instant eventTime = Instant.now();

        HistoryEvent fullEvent = event.toBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setSequenceId(nextSequenceId)
            .setWorkflowId(workflowId)
            .setEventTime(toProtoTimestamp(eventTime))
            .build();

        EventHistoryEntity entity = EventHistoryEntity.builder()
            .workflowId(workflowId)
            .sequenceId(nextSequenceId)
            .eventType(eventType)
            .eventPayload(serialize(fullEvent))
            .eventTime(eventTime)
            .build();

        eventHistoryRepository.save(entity);
        log.info("Appended {} event for workflow {} (sequenceId={})",
            eventType, workflowId, nextSequenceId);
    }

    private void evaluate(String workflowId) {
        List<EventHistoryEntity> history =
            eventHistoryRepository.findAllByWorkflowIdOrderedBySequenceId(workflowId);
        WorkflowState state = workflowStateMachine.replay(history);

        if (state.isFinished()) {
            WorkflowExecutionEntity workflow = workflowExecutionRepository.findById(workflowId)
                .orElseThrow(() -> new WorkflowNotFoundException(workflowId));
            workflow.setStatus(state.status());
            workflow.setEndTime(Instant.now());
            workflowExecutionRepository.save(workflow);
            log.info("Workflow {} reached terminal state {}", workflowId, state.status());
        }
    }

    private void ensureWorkflowExists(String workflowId) {
        if (!workflowExecutionRepository.existsById(workflowId)) {
            throw new WorkflowNotFoundException(workflowId);
        }
    }

    private String serialize(HistoryEvent event) {
        try {
            return JSON_PRINTER.print(event);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to serialize HistoryEvent to JSON", e);
        }
    }

    private static Timestamp toProtoTimestamp(Instant instant) {
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }
}
