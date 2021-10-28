package com.eventstore.dbclient;

import com.eventstore.dbclient.proto.shared.Shared;
import com.eventstore.dbclient.proto.streams.StreamsGrpc;
import com.eventstore.dbclient.proto.streams.StreamsOuterClass;
import io.grpc.Metadata;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    public Publisher<ResolvedEvent> execute() {
        CompletableFuture<Publisher<ResolvedEvent>> publisherFuture = this.client.run(channel -> {
            StreamsOuterClass.ReadReq request = StreamsOuterClass.ReadReq.newBuilder()
                    .setOptions(createOptions())
                    .build();

            Metadata headers = this.metadata;
            StreamsGrpc.StreamsStub client = MetadataUtils.attachHeaders(StreamsGrpc.newStub(channel), headers);

            Publisher<ResolvedEvent> eventPublisher = subscriber -> {
                final CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
                ClientResponseObserver<StreamsOuterClass.ReadReq, StreamsOuterClass.ReadResp> clientResponseObserver = new ClientReadResponseObserver(subscriptionFuture, subscriber);
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
