package io.chronos.server.engine;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.chronos.proto.HistoryEvent;
import io.chronos.server.domain.EventType;
import io.chronos.server.domain.WorkflowState;
import io.chronos.server.domain.WorkflowStatus;
import io.chronos.server.entity.EventHistoryEntity;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Pure Java component that replays a workflow's append-only event history to
 * derive its current state.
 *
 * <p>The state machine performs no I/O itself; it consumes the (already
 * persisted) {@link EventHistoryEntity} list and reduces it to a
 * {@link WorkflowState}. The status derivation rules are:
 *
 * <ul>
 *   <li>{@link WorkflowStatus#FAILED} when any task has failed;</li>
 *   <li>{@link WorkflowStatus#COMPLETED} when the workflow has started, at
 *       least one task was scheduled, and every scheduled task completed;</li>
 *   <li>{@link WorkflowStatus#RUNNING} otherwise.</li>
 * </ul>
 */
@Slf4j
@Component
public class WorkflowStateMachine {

    private static final JsonFormat.Parser JSON_PARSER =
        JsonFormat.parser().ignoringUnknownFields();

    /**
     * Replays the given events (in ascending sequence order) and returns the
     * derived workflow state.
     *
     * @param events the workflow's history events (may be unsorted)
     * @return the computed {@link WorkflowState}
     */
    public WorkflowState replay(List<EventHistoryEntity> events) {
        if (events == null || events.isEmpty()) {
            return new WorkflowState(WorkflowStatus.RUNNING, Set.of(), Set.of(), Set.of());
        }

        List<EventHistoryEntity> ordered = events.stream()
            .filter(Objects::nonNull)
            .sorted(Comparator.comparing(
                EventHistoryEntity::getSequenceId,
                Comparator.nullsLast(Comparator.naturalOrder())))
            .toList();

        boolean started = false;
        Set<String> scheduled = new LinkedHashSet<>();
        Set<String> completed = new LinkedHashSet<>();
        Set<String> failed = new LinkedHashSet<>();

        for (EventHistoryEntity event : ordered) {
            EventType type = event.getEventType();
            if (type == null) {
                log.warn("Skipping event with null type (workflow={}, sequenceId={})",
                    event.getWorkflowId(), event.getSequenceId());
                continue;
            }

            String taskId = extractTaskId(event);
            switch (type) {
                case WORKFLOW_EXECUTION_STARTED -> started = true;
                case TASK_SCHEDULED -> {
                    if (taskId != null) {
                        scheduled.add(taskId);
                    }
                }
                case TASK_COMPLETED -> {
                    if (taskId != null) {
                        completed.add(taskId);
                    }
                }
                case TASK_FAILED -> {
                    if (taskId != null) {
                        failed.add(taskId);
                    }
                }
            }
        }

        WorkflowStatus status = determineStatus(started, scheduled, completed, failed);
        return new WorkflowState(status, scheduled, completed, failed);
    }

    private WorkflowStatus determineStatus(
        boolean started,
        Set<String> scheduled,
        Set<String> completed,
        Set<String> failed
    ) {
        if (!failed.isEmpty()) {
            return WorkflowStatus.FAILED;
        }
        if (started && !scheduled.isEmpty() && completed.containsAll(scheduled)) {
            return WorkflowStatus.COMPLETED;
        }
        return WorkflowStatus.RUNNING;
    }

    /**
     * Extracts the task id from a task event's JSON payload, or {@code null}
     * when the event carries no task id (e.g. a started event) or the payload
     * cannot be parsed.
     */
    private String extractTaskId(EventHistoryEntity event) {
        String payload = event.getEventPayload();
        if (payload == null || payload.isBlank()) {
            return null;
        }

        try {
            HistoryEvent.Builder builder = HistoryEvent.newBuilder();
            JSON_PARSER.merge(payload, builder);
            HistoryEvent historyEvent = builder.build();

            String taskId = switch (historyEvent.getEventCase()) {
                case TASK_SCHEDULED -> historyEvent.getTaskScheduled().getTaskId();
                case TASK_COMPLETED -> historyEvent.getTaskCompleted().getTaskId();
                case TASK_FAILED -> historyEvent.getTaskFailed().getTaskId();
                case WORKFLOW_EXECUTION_STARTED, EVENT_NOT_SET -> "";
            };
            return taskId == null || taskId.isBlank() ? null : taskId;
        } catch (InvalidProtocolBufferException e) {
            log.warn("Failed to parse event payload (workflow={}, sequenceId={}): {}",
                event.getWorkflowId(), event.getSequenceId(), e.getMessage());
            return null;
        }
    }
}
