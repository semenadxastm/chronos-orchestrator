package io.chronos.server.entity;

import io.chronos.server.domain.WorkflowStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity representing a durable workflow execution.
 *
 * <p>This is the root aggregate of the event store. Its lifecycle status
 * ({@link WorkflowStatus}) is derived by replaying the append-only
 * {@link EventHistoryEntity} log through the workflow state machine.
 */
@Entity
@Table(name = "workflow_execution")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowExecutionEntity {

    /** Unique identifier (UUID) for this execution. */
    @Id
    @Column(name = "workflow_id", nullable = false, updatable = false, length = 36)
    private String workflowId;

    /** Logical type/name of the workflow, e.g. "ORDER_CHECKOUT". */
    @Column(name = "workflow_type", nullable = false)
    private String workflowType;

    /** Current lifecycle status. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private WorkflowStatus status;

    /** UTC instant at which this execution started. */
    @Column(name = "start_time", nullable = false, updatable = false)
    private Instant startTime;

    /** UTC instant at which this execution finished (null while RUNNING). */
    @Column(name = "end_time")
    private Instant endTime;

    /** Defaults {@link #startTime} to "now" (UTC) when not supplied at insert time. */
    @PrePersist
    void onCreate() {
        if (startTime == null) {
            startTime = Instant.now();
        }
    }
}
