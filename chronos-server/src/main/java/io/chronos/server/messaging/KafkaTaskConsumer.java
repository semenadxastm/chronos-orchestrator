package io.chronos.server.messaging;

import com.google.protobuf.InvalidProtocolBufferException;
import io.chronos.proto.TaskPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes scheduled tasks from the {@value KafkaTopics#TASKS_QUEUE} topic and
 * hands them off to the in-memory {@link TaskQueue} for gRPC polling.
 */
@Slf4j
@Component
public class KafkaTaskConsumer {

    private final TaskQueue taskQueue;

    public KafkaTaskConsumer(TaskQueue taskQueue) {
        this.taskQueue = taskQueue;
    }

    /**
     * Deserializes an incoming Kafka record into a {@link TaskPayload} and
     * enqueues it. Malformed records are logged and skipped rather than
     * crashing the consumer.
     */
    @KafkaListener(topics = KafkaTopics.TASKS_QUEUE)
    public void onTask(ConsumerRecord<String, byte[]> record) {
        try {
            TaskPayload task = TaskPayload.parseFrom(record.value());
            log.debug("Received task {} (type={}) from Kafka partition={} offset={}",
                task.getTaskId(), task.getTaskType(), record.partition(), record.offset());

            if (taskQueue.enqueue(task)) {
                log.info("Buffered task {} (type={}) for dispatch", task.getTaskId(), task.getTaskType());
            } else {
                log.warn("Task queue rejected task {} (type={})", task.getTaskId(), task.getTaskType());
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to deserialize task from Kafka (partition={}, offset={}): {}",
                record.partition(), record.offset(), e.getMessage());
        }
    }
}
