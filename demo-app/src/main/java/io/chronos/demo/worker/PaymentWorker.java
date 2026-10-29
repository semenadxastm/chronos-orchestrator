package io.chronos.demo.worker;

import io.chronos.client.TaskExecutor;
import io.chronos.client.WorkerRegistry;
import io.chronos.proto.TaskPayload;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles {@code PROCESS_PAYMENT} tasks. Simulates an external payment gateway
 * call with a sleep, then reports success.
 */
@Slf4j
@Component
public class PaymentWorker implements TaskExecutor {

    public static final String TASK_TYPE = "PROCESS_PAYMENT";

    public PaymentWorker(WorkerRegistry workerRegistry) {
        workerRegistry.register(TASK_TYPE, this);
    }

    @Override
    public byte[] execute(TaskPayload payload) throws Exception {
        log.info("[PaymentWorker] Charging card for task {} (workflow {})",
            payload.getTaskId(), payload.getWorkflowId());

        Thread.sleep(2_000L); // simulate external payment gateway latency

        log.info("[PaymentWorker] Payment processed for task {}", payload.getTaskId());
        return "PAYMENT_OK".getBytes(StandardCharsets.UTF_8);
    }
}
