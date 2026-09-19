package io.chronos.server.repository;

import io.chronos.server.entity.EventHistoryEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data repository for {@link EventHistoryEntity}.
 *
 * <p>Provides queries over the append-only event log. Events are always
 * consumed in ascending {@code sequenceId} order within a workflow.
 */
public interface EventHistoryRepository extends JpaRepository<EventHistoryEntity, Long> {

    /**
     * Fetches the complete, ordered history for a workflow. This is the query
     * used when replaying events through the state machine.
     *
     * @param workflowId the owning workflow execution id
     * @return all events for the workflow, ordered by sequence id ascending
     */
    @Query("SELECT e FROM EventHistoryEntity e WHERE e.workflowId = :workflowId ORDER BY e.sequenceId ASC")
    List<EventHistoryEntity> findAllByWorkflowIdOrderedBySequenceId(@Param("workflowId") String workflowId);

    /**
     * Returns the highest sequence id currently recorded for a workflow, or 0
     * when no events exist yet. Used to compute the next sequence when
     * appending a new event.
     *
     * @param workflowId the owning workflow execution id
     * @return the current max sequence id, or 0 if the workflow has no events
     */
    @Query("SELECT COALESCE(MAX(e.sequenceId), 0) FROM EventHistoryEntity e WHERE e.workflowId = :workflowId")
    Long findMaxSequenceIdByWorkflowId(@Param("workflowId") String workflowId);
}
