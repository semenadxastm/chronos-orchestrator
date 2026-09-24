package io.chronos.server.messaging;

import io.chronos.proto.TaskPayload;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes scheduled tasks to the {@value KafkaTopics#TASKS_QUEUE} topic.
 *
 * <p>The protobuf {@link TaskPayload} is serialized to raw bytes
 * ({@code TaskPayload#toByteArray()}) and keyed by task id so that all records
 * for a given task land on the same partition.
 */
@Slf4j
@Component
public class KafkaTaskPublisher {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaTaskPublisher(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Asynchronously publishes a scheduled task.
     *
     * @param payload the task to publish
     * @return a future that completes once the record has been acknowledged
     * @throws IllegalArgumentException if {@code payload} is null
     */
    public CompletableFuture<SendResult<String, byte[]>> publish(TaskPayload payload) {
        if (payload == null) {
            throw new IllegalArgumentException("TaskPayload must not be null");
        }

        String key = payload.getTaskId();
        byte[] value = payload.toByteArray();

        log.debug("Publishing task {} (type={}) to topic {}",
            key, payload.getTaskType(), KafkaTopics.TASKS_QUEUE);

        return kafkaTemplate.send(KafkaTopics.TASKS_QUEUE, key, value)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish task {} to topic {}",
                        key, KafkaTopics.TASKS_QUEUE, ex);
                } else if (result != null) {
                    log.info("Published task {} to topic {} partition={} offset={}",
                        key, KafkaTopics.TASKS_QUEUE,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
    }
}
