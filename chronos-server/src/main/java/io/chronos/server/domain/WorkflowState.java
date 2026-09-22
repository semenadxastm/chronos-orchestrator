package io.chronos.server.domain;

import java.util.Set;

/**
 * Immutable snapshot of a workflow execution's derived state after replaying
 * its append-only event history.
 *
 * @param status           the computed lifecycle status
 * @param scheduledTaskIds task ids that have been scheduled
 * @param completedTaskIds task ids that have been completed
 * @param failedTaskIds    task ids that have failed
 */
public record WorkflowState(
    WorkflowStatus status,
    Set<String> scheduledTaskIds,
    Set<String> completedTaskIds,
    Set<String> failedTaskIds
) {

    /** Defensive copy so the record is deeply immutable. */
    public WorkflowState {
        scheduledTaskIds = Set.copyOf(scheduledTaskIds);
        completedTaskIds = Set.copyOf(completedTaskIds);
        failedTaskIds = Set.copyOf(failedTaskIds);
    }

    /** True when the workflow has reached a terminal state. */
    public boolean isFinished() {
        return status == WorkflowStatus.COMPLETED || status == WorkflowStatus.FAILED;
    }
}
