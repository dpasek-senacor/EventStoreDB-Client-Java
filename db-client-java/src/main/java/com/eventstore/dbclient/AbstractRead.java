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

public abstract class AbstractRead {
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

    public CompletableFuture<Publisher<ResolvedEvent>> execute() {
        return this.client.run(channel -> {
            StreamsOuterClass.ReadReq request = StreamsOuterClass.ReadReq.newBuilder()
                    .setOptions(createOptions())
                    .build();

            Metadata headers = this.metadata;
            StreamsGrpc.StreamsStub client = MetadataUtils.attachHeaders(StreamsGrpc.newStub(channel), headers);

            Publisher<ResolvedEvent> eventPublisher = subscriber -> {
                client.read(request, new ClientResponseObserver<StreamsOuterClass.ReadReq, StreamsOuterClass.ReadResp>() {

                    @Override
                    public void beforeStart(ClientCallStreamObserver<StreamsOuterClass.ReadReq> requestStream) {
                        requestStream.disableAutoRequestWithInitial(0);
                        subscriber.onSubscribe(new Subscription() {
                            @Override
                            public void request(long n) {
                                requestStream.request((int) Math.min(n, (long) Integer.MAX_VALUE));
                            }

                            @Override
                            public void cancel() {
                                requestStream.cancel("Subscription was cancelled", null);
                            }
                        });
                    }

                    @Override
                    public void onNext(StreamsOuterClass.ReadResp value) {
                        if (value.hasEvent()) {
                            try {
                                subscriber.onNext(ResolvedEvent.fromWire(value.getEvent()));
                            } catch (Throwable t) {
                                subscriber.onError(t);
                            }
                        }
                    }

                    @Override
                    public void onCompleted() {
                        try {
                            subscriber.onComplete();
                        } catch (Throwable t) {
                            subscriber.onError(t);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        if (t instanceof StatusRuntimeException) {
                            StatusRuntimeException e = (StatusRuntimeException) t;
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
                });
            };
            return CompletableFuture.supplyAsync(() -> eventPublisher);
        });
    }
}
