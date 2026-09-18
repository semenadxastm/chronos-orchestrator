package io.chronos.server.entity;

import io.chronos.server.domain.EventType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * JPA entity representing a single immutable entry in a workflow's append-only
 * event log.
 *
 * <p>Events are ordered by {@link #sequenceId} within a workflow and are never
 * updated after being written. The {@link #workflowExecution} association
 * exists to enforce the foreign-key relationship at the schema level, while
 * {@link #workflowId} is the denormalized column used for efficient queries.
 */
@Entity
@Table(
    name = "event_history",
    indexes = {
        @Index(name = "uk_event_history_workflow_seq", columnList = "workflow_id, sequence_id", unique = true)
    })
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventHistoryEntity {

    /** Surrogate primary key. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    /** Owning workflow execution id (UUID). Denormalized for efficient queries. */
    @Column(name = "workflow_id", nullable = false, length = 36)
    private String workflowId;

    /**
     * Foreign key to the owning workflow execution. Read-only: the column is
     * owned by {@link #workflowId}, this association only declares the FK.
     */
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "workflow_id", referencedColumnName = "workflow_id", insertable = false, updatable = false)
    private WorkflowExecutionEntity workflowExecution;

    /** Monotonic sequence number within the owning workflow (1-based). */
    @Column(name = "sequence_id", nullable = false)
    private Long sequenceId;

    /** Denormalized event kind for fast filtering and replay. */
    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 32)
    private EventType eventType;

    /** Serialized (JSON) event payload. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "event_payload", columnDefinition = "jsonb")
    private String eventPayload;

    /** UTC instant at which this event was recorded. */
    @Column(name = "event_time", nullable = false)
    private Instant eventTime;

    /** Defaults {@link #eventTime} to "now" (UTC) when not supplied at insert time. */
    @PrePersist
    void onCreate() {
        if (eventTime == null) {
            eventTime = Instant.now();
        }
    }
}
