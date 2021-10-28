package com.eventstore.dbclient;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class EventStoreDBClient extends EventStoreDBClientBase {
    private EventStoreDBClient(EventStoreDBClientSettings settings) {
        super(settings);
    }

    public static EventStoreDBClient create(EventStoreDBClientSettings settings) {
        return new EventStoreDBClient(settings);
    }

    public CompletableFuture<WriteResult> appendToStream(String streamName, EventData... events) {
        return this.appendToStream(streamName, Arrays.stream(events).iterator());
    }

    public CompletableFuture<WriteResult> appendToStream(String streamName, Iterator<EventData> events) {
        return this.appendToStream(streamName, AppendToStreamOptions.get(), events);
    }

    public CompletableFuture<WriteResult> appendToStream(String streamName, AppendToStreamOptions options, EventData... events) {
        return this.appendToStream(streamName, options, Arrays.stream(events).iterator());
    }

    public CompletableFuture<WriteResult> appendToStream(String streamName, AppendToStreamOptions options, Iterator<EventData> events) {
        if (options == null)
            options = AppendToStreamOptions.get();

        if (!options.hasUserCredentials())
            options.authenticated(this.credentials);

        return new AppendToStream(this.client, streamName, events, options).execute();
    }

    public CompletableFuture<WriteResult> setStreamMetadata(String streamName, StreamMetadata metadata) {
        return setStreamMetadata(streamName, null, metadata);
    }

    public CompletableFuture<WriteResult> setStreamMetadata(String streamName, AppendToStreamOptions options, StreamMetadata metadata) {
        EventData event = EventDataBuilder.json("$metadata", metadata.serialize()).build();

        return appendToStream("$$" + streamName, options, event);
    }

    public Publisher<ResolvedEvent> readStream(String streamName) {
        return this.readStream(streamName, Long.MAX_VALUE, ReadStreamOptions.get());
    }

    public Publisher<ResolvedEvent> readStream(String streamName, long maxCount) {
        return this.readStream(streamName, maxCount, ReadStreamOptions.get());
    }

    public Publisher<ResolvedEvent> readStream(String streamName, ReadStreamOptions options) {
        return this.readStream(streamName, Long.MAX_VALUE, options);
    }

    public Publisher<ResolvedEvent> readStream(String streamName, long maxCount, ReadStreamOptions options) {
        if (options == null)
            options = ReadStreamOptions.get();

        if (!options.hasUserCredentials())
            options.authenticated(this.credentials);

        return new ReadStream(this.client, streamName, maxCount, options);
    }

    public CompletableFuture<StreamMetadata> getStreamMetadata(String streamName) {
        return getStreamMetadata(streamName, null);
    }

    static class StreamMetadataSubscriber implements Subscriber<ResolvedEvent> {

        private StreamMetadata metadata = new StreamMetadata();
        private final CompletableFuture<StreamMetadata> future = new CompletableFuture<>();

        @Override
        public void onSubscribe(org.reactivestreams.Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ResolvedEvent event) {
            try {
                HashMap<String, Object> source = event.getOriginalEvent().getEventDataAs(HashMap.class);
                metadata = StreamMetadata.deserialize(source);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onComplete() {
            future.complete(metadata);
        }

        @Override
        public void onError(Throwable error) {
            future.completeExceptionally(error);
        }

        public CompletableFuture<StreamMetadata> getStreamMetadata() {
            return future;
        }
    }

    public CompletableFuture<StreamMetadata> getStreamMetadata(String streamName, ReadStreamOptions options) {
        final StreamMetadataSubscriber streamMetadataSubscriber = new StreamMetadataSubscriber();
        readStream("$$" + streamName, options).subscribe(streamMetadataSubscriber);
        return streamMetadataSubscriber.getStreamMetadata().exceptionally(e -> {
            Throwable reason = e.getCause() != null ? e.getCause() : e;
            if (reason instanceof StreamNotFoundException) {
                return new StreamMetadata();
            }

            throw new RuntimeException(e);
        });
    }

    public Publisher<ResolvedEvent> readAll() {
        return this.readAll(Long.MAX_VALUE, ReadAllOptions.get());
    }

    public Publisher<ResolvedEvent> readAll(long maxCount) {
        return this.readAll(maxCount, ReadAllOptions.get());
    }

    public Publisher<ResolvedEvent> readAll(ReadAllOptions options) {
        return this.readAll(Long.MAX_VALUE, options);
    }

    public Publisher<ResolvedEvent> readAll(long maxCount, ReadAllOptions options) {
        if (options == null)
            options = ReadAllOptions.get();

        if (!options.hasUserCredentials())
            options.authenticated(this.credentials);

        return new ReadAll(this.client, maxCount, options);
    }

    public CompletableFuture<Subscription> subscribeToStream(String streamName, SubscriptionListener listener) {
        return this.subscribeToStream(streamName, listener, SubscribeToStreamOptions.get());
    }

    public CompletableFuture<Subscription> subscribeToStream(String streamName, SubscriptionListener
            listener, SubscribeToStreamOptions options) {
        if (options == null)
            options = SubscribeToStreamOptions.get();

        if (!options.hasUserCredentials())
            options.authenticated(this.credentials);

        return new SubscribeToStream(this.client, streamName, listener, options).execute();
    }

    public CompletableFuture<Subscription> subscribeToAll(SubscriptionListener listener) {
        return this.subscribeToAll(listener, SubscribeToAllOptions.get());
    }

    public CompletableFuture<Subscription> subscribeToAll(SubscriptionListener listener, SubscribeToAllOptions
            options) {
        if (options == null)
            options = SubscribeToAllOptions.get();

        if (!options.hasUserCredentials())
            options.authenticated(this.credentials);

        return new SubscribeToAll(this.client, listener, options).execute();
    }

    public CompletableFuture<DeleteResult> deleteStream(String streamName) {
        return this.deleteStream(streamName, DeleteStreamOptions.get());
    }

    public CompletableFuture<DeleteResult> deleteStream(String streamName, DeleteStreamOptions options) {
        if (options == null)
            options = DeleteStreamOptions.get();

        if (!options.hasUserCredentials())
            options.authenticated(this.credentials);

        return new DeleteStream(this.client, streamName, options).execute();
    }
}
