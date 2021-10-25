package com.eventstore.dbclient;

import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

        try {
            return new ReadStream(this.client, streamName, maxCount, options).execute().get();
        } catch (InterruptedException e) {

        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        throw new RuntimeException();
    }

    public Publisher<StreamMetadata> getStreamMetadata(String streamName) {
        return getStreamMetadata(streamName, null);
    }

    static class StreamMetadataReadObserver extends ReadObserver<StreamMetadata> {
        StreamMetadata metadata;

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
        public void onStreamNotFound() {
            metadata = new StreamMetadata();
        }

        @Override
        public StreamMetadata onCompleted() {
            return metadata;
        }

        @Override
        public void onError(Throwable error) {

        }

        public StreamMetadata getStreamMetadata() {
            return metadata;
        }
    }

    public Publisher<StreamMetadata> getStreamMetadata(String streamName, ReadStreamOptions options) {
        return Flowable.fromPublisher(readStream("$$" + streamName, options)).map(event ->
                StreamMetadata.deserialize(event.getOriginalEvent().getEventDataAs(HashMap.class)));
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

        return new ReadAll(this.client, maxCount, options).execute().join();
    }

    public CompletableFuture<Subscription> subscribeToStream(String streamName, SubscriptionListener listener) {
        return this.subscribeToStream(streamName, listener, SubscribeToStreamOptions.get());
    }

    public CompletableFuture<Subscription> subscribeToStream(String streamName, SubscriptionListener listener, SubscribeToStreamOptions options) {
        if (options == null)
            options = SubscribeToStreamOptions.get();

        if (!options.hasUserCredentials())
            options.authenticated(this.credentials);

        return new SubscribeToStream(this.client, streamName, listener, options).execute();
    }

    public CompletableFuture<Subscription> subscribeToAll(SubscriptionListener listener) {
        return this.subscribeToAll(listener, SubscribeToAllOptions.get());
    }

    public CompletableFuture<Subscription> subscribeToAll(SubscriptionListener listener, SubscribeToAllOptions options) {
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
