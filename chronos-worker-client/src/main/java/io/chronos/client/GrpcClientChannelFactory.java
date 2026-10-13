package io.chronos.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates and owns a persistent, multiplexed gRPC {@link ManagedChannel} to the
 * chronos-server.
 *
 * <p>A single {@link ManagedChannel} is lazily created on first use and reused
 * for all subsequent calls. gRPC multiplexes many concurrent RPCs over the one
 * underlying HTTP/2 connection, so a single channel safely serves multiple
 * polling threads and stub instances.
 *
 * <p>Plaintext transport is used for local/development deployments; production
 * deployments should enable TLS via
 * {@link ManagedChannelBuilder#useTransportSecurity()}.
 */
@Slf4j
public class GrpcClientChannelFactory implements AutoCloseable {

    private final String host;
    private final int port;

    /** Lazily-initialized, shared channel. Guarded by double-checked locking. */
    private volatile ManagedChannel channel;

    public GrpcClientChannelFactory(String host, int port) {
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = port;
    }

    /**
     * Returns the shared channel, creating it on first call.
     *
     * @return the persistent {@link ManagedChannel}
     */
    public ManagedChannel getChannel() {
        ManagedChannel local = channel;
        if (local == null) {
            synchronized (this) {
                local = channel;
                if (local == null) {
                    log.info("Creating gRPC channel to {}:{}", host, port);
                    local = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .build();
                    channel = local;
                }
            }
        }
        return local;
    }

    /**
     * Gracefully shuts the channel down and clears it so a subsequent
     * {@link #getChannel()} call can build a fresh connection.
     */
    @Override
    public void close() {
        ManagedChannel local = channel;
        if (local != null) {
            log.info("Shutting down gRPC channel to {}:{}", host, port);
            local.shutdown();
            channel = null;
        }
    }
}
