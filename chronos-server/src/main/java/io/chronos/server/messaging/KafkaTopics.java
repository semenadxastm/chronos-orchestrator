package io.chronos.server.messaging;

/**
 * Central registry of Kafka topic names used by the Chronos server.
 */
public final class KafkaTopics {

    /** Queue of scheduled tasks, produced by the server and consumed by workers. */
    public static final String TASKS_QUEUE = "chronos.tasks.queue";

    private KafkaTopics() {
        // utility class - no instances
    }
}
