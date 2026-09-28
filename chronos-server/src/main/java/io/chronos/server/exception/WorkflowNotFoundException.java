package io.chronos.server.exception;

/**
 * Thrown when an operation targets a workflow execution that does not exist.
 */
public class WorkflowNotFoundException extends RuntimeException {

    public WorkflowNotFoundException(String workflowId) {
        super("Workflow execution not found: " + workflowId);
    }
}
