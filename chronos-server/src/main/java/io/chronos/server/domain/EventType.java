package io.chronos.server.domain;

/**
 * The distinct kinds of events persisted in a workflow's append-only history.
 */
public enum EventType {
    WORKFLOW_EXECUTION_STARTED,
    TASK_SCHEDULED,
    TASK_COMPLETED,
    TASK_FAILED
}
