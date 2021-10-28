package com.eventstore.dbclient;

import com.eventstore.dbclient.proto.shared.Shared;
import com.eventstore.dbclient.proto.streams.StreamsGrpc;
import com.eventstore.dbclient.proto.streams.StreamsOuterClass;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractRead {

    private static final String MANUAL_CANCELLATION_MESSAGE = "Subscription was manually cancelled";

    protected static final StreamsOuterClass.ReadReq.Options.Builder defaultReadOptions;

    private final GrpcClient client;
    protected final Metadata metadata;

    protected AbstractRead(GrpcClient client, Metadata metadata) {
        this.client = client;
        this.metadata = metadata;
    }

    static {
        defaultReadOptions = StreamsOuterClass.ReadReq.Options.newBuilder()
                .setUuidOption(StreamsOuterClass.ReadReq.Options.UUIDOption.newBuilder()
                        .setStructured(Shared.Empty.getDefaultInstance()));
    }

    public abstract StreamsOuterClass.ReadReq.Options.Builder createOptions();

    public Publisher<ResolvedEvent> execute() {
        CompletableFuture<Publisher<ResolvedEvent>> publisherFuture = this.client.run(channel -> {
            StreamsOuterClass.ReadReq request = StreamsOuterClass.ReadReq.newBuilder()
                    .setOptions(createOptions())
                    .build();

            Metadata headers = this.metadata;
            StreamsGrpc.StreamsStub client = MetadataUtils.attachHeaders(StreamsGrpc.newStub(channel), headers);

            Publisher<ResolvedEvent> eventPublisher = subscriber -> {
                final CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
                ClientResponseObserver<StreamsOuterClass.ReadReq, StreamsOuterClass.ReadResp> clientResponseObserver = new ClientResponseObserver<StreamsOuterClass.ReadReq, StreamsOuterClass.ReadResp>() {
                    private boolean completed;

                    @Override
                    public void beforeStart(ClientCallStreamObserver<StreamsOuterClass.ReadReq> requestStream) {
                        requestStream.disableAutoRequestWithInitial(0);

                        subscriptionFuture.complete(new Subscription() {
                            @Override
                            public void request(long n) {
                                try {
                                    if (n < 1) {
                                        throw new IllegalArgumentException("Number requested must be positive");
                                    }
                                    requestStream.request((int) Math.min(n, Integer.MAX_VALUE));
                                } catch (Throwable t) {
                                    subscriber.onError(t);
                                    completed = true;
                                }
                            }

                            @Override
                            public void cancel() {
                                requestStream.cancel(MANUAL_CANCELLATION_MESSAGE, null);
                            }
                        });
                    }

                    @Override
                    public void onNext(StreamsOuterClass.ReadResp value) {
                        if (value.hasStreamNotFound()) {
                            subscriber.onError(new StreamNotFoundException());
                            completed = true;
                            return;
                        }
                        if (value.hasEvent()) {
                            try {
                                subscriber.onNext(ResolvedEvent.fromWire(value.getEvent()));
                            } catch (Throwable t) {
                                subscriber.onError(t);
                                completed = true;
                            }
                        }
                    }

                    @Override
                    public void onCompleted() {
                        if (completed) {
                            return;
                        }

                        subscriber.onComplete();
                    }

                    @Override
                    public void onError(Throwable t) {
                        if (completed) {
                            return;
                        }

                        if (t instanceof StatusRuntimeException) {
                            StatusRuntimeException e = (StatusRuntimeException) t;
                            if (e.getStatus() != null && MANUAL_CANCELLATION_MESSAGE.equals(e.getStatus().getDescription())) {
                                return;
                            }
                            String leaderHost = e.getTrailers().get(Metadata.Key.of("leader-endpoint-host", Metadata.ASCII_STRING_MARSHALLER));
                            String leaderPort = e.getTrailers().get(Metadata.Key.of("leader-endpoint-port", Metadata.ASCII_STRING_MARSHALLER));

                            if (leaderHost != null && leaderPort != null) {
                                NotLeaderException reason = new NotLeaderException(leaderHost, Integer.valueOf(leaderPort));
                                subscriber.onError(reason);
                                return;
                            }
                        }

                        subscriber.onError(t);
                    }
                };
                client.read(request, clientResponseObserver);
                try {
                    Subscription subscription = subscriptionFuture.get(0, TimeUnit.SECONDS);
                    subscriber.onSubscribe(subscription);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            };
            return CompletableFuture.completedFuture(eventPublisher);
        });
        return publisherFuture.join();
    }
}
