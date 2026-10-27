package io.chronos.demo.config;

import io.chronos.client.GrpcClientChannelFactory;
import io.chronos.client.TaskExecutionPipeline;
import io.chronos.client.WorkerPollingEngine;
import io.chronos.client.WorkerRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the chronos worker-client SDK beans into the Spring context.
 */
@Configuration
public class ChronosWorkerConfiguration {

    @Bean
    public GrpcClientChannelFactory grpcClientChannelFactory(
        @Value("${chronos.grpc.host}") String host,
        @Value("${chronos.grpc.port}") int port
    ) {
        return new GrpcClientChannelFactory(host, port);
    }

    @Bean
    public WorkerRegistry workerRegistry() {
        return new WorkerRegistry();
    }

    @Bean
    public TaskExecutionPipeline taskExecutionPipeline(
        WorkerRegistry workerRegistry,
        GrpcClientChannelFactory channelFactory
    ) {
        return new TaskExecutionPipeline(workerRegistry, channelFactory);
    }

    @Bean
    public WorkerPollingEngine workerPollingEngine(
        GrpcClientChannelFactory channelFactory,
        TaskExecutionPipeline pipeline,
        @Value("${chronos.worker.poller-count:2}") int pollerCount,
        @Value("${chronos.worker.poll-timeout-ms:5000}") long pollTimeoutMs
    ) {
        return new WorkerPollingEngine(channelFactory, pipeline)
            .withPollerCount(pollerCount)
            .withPollTimeoutMs(pollTimeoutMs);
    }
}
