package io.chronos.server.repository;

import io.chronos.server.entity.WorkflowExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data repository for {@link WorkflowExecutionEntity}.
 *
 * <p>The identifier type is {@code String} because the entity's primary key is
 * the workflow UUID stored as a string.
 */
public interface WorkflowExecutionRepository extends JpaRepository<WorkflowExecutionEntity, String> {
}
