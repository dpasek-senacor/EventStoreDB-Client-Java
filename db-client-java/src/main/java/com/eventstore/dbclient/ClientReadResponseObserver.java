package com.eventstore.dbclient;

import com.eventstore.dbclient.proto.streams.StreamsOuterClass;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;

class ClientReadResponseObserver implements ClientResponseObserver<StreamsOuterClass.ReadReq, StreamsOuterClass.ReadResp> {

    private static final String MANUAL_CANCELLATION_MESSAGE = "Subscription was manually cancelled";

    private final CompletableFuture<Subscription> subscriptionFuture;
    private final Subscriber<? super ResolvedEvent> subscriber;

    private boolean completed;

    public ClientReadResponseObserver(CompletableFuture<Subscription> subscriptionFuture, Subscriber<? super ResolvedEvent> subscriber) {
        this.subscriptionFuture = subscriptionFuture;
        this.subscriber = subscriber;
    }

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
                NotLeaderException reason = new NotLeaderException(leaderHost, Integer.parseInt(leaderPort));
                subscriber.onError(reason);
                return;
            }
        }

        subscriber.onError(t);
    }
}
