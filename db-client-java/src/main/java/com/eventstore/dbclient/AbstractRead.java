package com.eventstore.dbclient;

import com.eventstore.dbclient.proto.shared.Shared;
import com.eventstore.dbclient.proto.streams.StreamsGrpc;
import com.eventstore.dbclient.proto.streams.StreamsOuterClass;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractRead implements Publisher<ResolvedEvent> {

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

    @Override
    public void subscribe(Subscriber<? super ResolvedEvent> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException();
        }
        this.client.run(channel -> {
            StreamsOuterClass.ReadReq request = StreamsOuterClass.ReadReq.newBuilder()
                    .setOptions(createOptions())
                    .build();

            Metadata headers = this.metadata;
            StreamsGrpc.StreamsStub client = MetadataUtils.attachHeaders(StreamsGrpc.newStub(channel), headers);

            ClientReadResponseObserver clientResponseObserver = new ClientReadResponseObserver(subscriber);
            client.read(request, clientResponseObserver);
            subscriber.onSubscribe(clientResponseObserver.getSubscription());
            return CompletableFuture.completedFuture(this);
        });
    }

}
