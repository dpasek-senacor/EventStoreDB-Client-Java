package com.eventstore.dbclient;

import com.eventstore.dbclient.proto.streams.StreamsOuterClass;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

class ClientReadResponseObserver implements ClientResponseObserver<StreamsOuterClass.ReadReq, StreamsOuterClass.ReadResp> {

    private static final String MANUAL_CANCELLATION_MESSAGE = "Subscription was manually cancelled";

    private final Subscriber<? super ResolvedEvent> subscriber;

    private Subscription subscription;

    private boolean initialRequest = true;

    private boolean completed;

    public ClientReadResponseObserver(Subscriber<? super ResolvedEvent> subscriber) {
        this.subscriber = subscriber;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<StreamsOuterClass.ReadReq> requestStream) {
        requestStream.disableAutoRequestWithInitial(1);
        subscription = new Subscription() {
            @Override
            public void request(long n) {
                try {
                    if (n < 1) {
                        throw new IllegalArgumentException("non-positive subscription request: " + n);
                    }
                    long requested =  initialRequest ? n - 1 : n;
                    if (requested > 0) {
                        requestStream.request((int) Math.min(requested, Integer.MAX_VALUE));
                    }
                } catch (Throwable t) {
                    subscriber.onError(t);
                    completed = true;
                } finally {
                    initialRequest = false;
                }
            }

            @Override
            public void cancel() {
                requestStream.cancel(MANUAL_CANCELLATION_MESSAGE, null);
            }
        };
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
