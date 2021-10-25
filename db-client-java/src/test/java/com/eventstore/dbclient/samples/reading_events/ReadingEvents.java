package com.eventstore.dbclient.samples.reading_events;

import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ReadAllOptions;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.eventstore.dbclient.UserCredentials;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;


public class ReadingEvents {
    private static void readFromStream(EventStoreDBClient client) {
        // region read-from-stream
        ReadStreamOptions options = ReadStreamOptions.get()
                .forwards()
                .fromStart();

        client.readStream("some-stream", options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                RecordedEvent recordedEvent = event.getOriginalEvent();
                try {
                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });

        // endregion read-from-stream

        // region iterate-stream

        client.readStream("some-stream", options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                RecordedEvent recordedEvent = event.getOriginalEvent();
                try {
                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });

        // endregion iterate-stream
    }

    private static void readFromStreamPosition(EventStoreDBClient client) {
        // region read-from-stream-position
        ReadStreamOptions options = ReadStreamOptions.get()
                .forwards()
                .fromRevision(10);

        client.readStream("some-stream", 20, options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                RecordedEvent recordedEvent = event.getOriginalEvent();
                try {
                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });

        // endregion read-from-stream-position

        // region iterate-stream
        client.readStream("some-stream", 20, options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                RecordedEvent recordedEvent = event.getOriginalEvent();
                try {
                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });

        // endregion iterate-stream
    }

    private static void readStreamOverridingUserCredentials(EventStoreDBClient client) {

        // region overriding-user-credentials
        UserCredentials credentials = new UserCredentials("admin", "changeit");

        ReadStreamOptions options = ReadStreamOptions.get()
                .forwards()
                .fromStart()
                .authenticated(credentials);

        client.readStream("some-stream", options).subscribe(subscriber);
        // endregion overriding-user-credentials
    }

    private static void readFromStreamPositionCheck(EventStoreDBClient client) {
        // region checking-for-stream-presence
        ReadStreamOptions options = ReadStreamOptions.get()
                .forwards()
                .fromRevision(10);

        client.readStream("some-stream", 20, options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                // ...
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {
                if (error.getCause() != null && error.getCause() instanceof StreamNotFoundException) {
                    // handle stream not found
                }
            }
        });
        // endregion checking-for-stream-presence
    }

    private static void readFromStreamBackwards(EventStoreDBClient client) {
        // region reading-backwards
        ReadStreamOptions options = ReadStreamOptions.get()
                .backwards()
                .fromEnd();

        client.readStream("some-stream", options).subscribe(subscriber);
        // endregion reading-backwards
    }

    private static void readFromAllStream(EventStoreDBClient client) {
        // region read-from-all-stream
        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart();

        client.readAll().subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                // ...
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });
        // endregion read-from-all-stream

        // region read-from-all-stream-iterate
        client.readAll().subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                // ...
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });
        // endregion read-from-all-stream-iterate
    }

    private static void readAllOverridingUserCredentials(EventStoreDBClient client) {
        // region read-all-overriding-user-credentials
        UserCredentials credentials = new UserCredentials("admin", "changeit");

        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart()
                .authenticated(credentials);

        client.readAll(options).subscribe(subscriber);
        // endregion read-all-overriding-user-credentials
    }

    private static void ignoreSystemEvents(EventStoreDBClient client) {
        // region ignore-system-events
        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart();

        client.readAll(options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                if (!event.getOriginalEvent().getEventType().startsWith("$")) {
                    // ...
                }
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });
        // endregion ignore-system-events
    }

    private static void readFromAllStreamBackwards(EventStoreDBClient client) {
        // region read-from-all-stream-backwards
        ReadAllOptions options = ReadAllOptions.get()
                .backwards()
                .fromEnd();

        client.readAll(options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {

            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });
        // endregion read-from-all-stream-backwards

        // region read-from-all-stream-iterate

        client.readAll(options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ResolvedEvent event) {
                // Doing something...
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable error) {

            }
        });

        // endregion read-from-all-stream-iterate
    }

    private static void readFromStreamResolvingLinkTos(EventStoreDBClient client) {
        // region read-from-all-stream-resolving-link-Tos
        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart()
                .resolveLinkTos();

        client.readAll(options).subscribe(subscriber);

        // endregion read-from-all-stream-resolving-link-Tos
    }

    private final static Subscriber<ResolvedEvent> subscriber = new Subscriber<ResolvedEvent>() {

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ResolvedEvent event) {
        }

        @Override
        public void onComplete() {
        }

        @Override
        public void onError(Throwable error) {

        }
    };
}
