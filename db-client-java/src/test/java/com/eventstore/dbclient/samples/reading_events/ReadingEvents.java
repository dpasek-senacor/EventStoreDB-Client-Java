//package com.eventstore.dbclient.samples.reading_events;
//
//import com.eventstore.dbclient.*;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import java.util.List;
//import java.util.concurrent.ExecutionException;
//
//
//
//public class ReadingEvents {
//    private static void readFromStream(EventStoreDBClient client) throws ExecutionException, InterruptedException, JsonProcessingException {
//        // region read-from-stream
//        ReadStreamOptions options = ReadStreamOptions.get()
//                .forwards()
//                .fromStart();
//
//        client.readStream("some-stream", options, new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//                RecordedEvent recordedEvent = event.getOriginalEvent();
//                try {
//                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
//                } catch (JsonProcessingException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        }).get();
//
//        // endregion read-from-stream
//
//        // region iterate-stream
//
//        client.readStream("some-stream", options, new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//                RecordedEvent recordedEvent = event.getOriginalEvent();
//                try {
//                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
//                } catch (JsonProcessingException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        }).get();
//
//        // endregion iterate-stream
//    }
//
//    private static void readFromStreamPosition(EventStoreDBClient client) throws ExecutionException, InterruptedException, JsonProcessingException {
//        // region read-from-stream-position
//        ReadStreamOptions options = ReadStreamOptions.get()
//                .forwards()
//                .fromRevision(10);
//
//        client.readStream("some-stream", 20, options, new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//                RecordedEvent recordedEvent = event.getOriginalEvent();
//                try {
//                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
//                } catch (JsonProcessingException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        })
//        .get();
//
//        // endregion read-from-stream-position
//
//        // region iterate-stream
//        client.readStream("some-stream", 20, options, new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//                RecordedEvent recordedEvent = event.getOriginalEvent();
//                try {
//                    System.out.println(new ObjectMapper().writeValueAsString(recordedEvent.getEventData()));
//                } catch (JsonProcessingException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        })
//        .get();
//
//        // endregion iterate-stream
//    }
//
//    private static void readStreamOverridingUserCredentials(EventStoreDBClient client) throws ExecutionException, InterruptedException {
//
//        // region overriding-user-credentials
//        UserCredentials credentials = new UserCredentials("admin", "changeit");
//
//        ReadStreamOptions options = ReadStreamOptions.get()
//                .forwards()
//                .fromStart()
//                .authenticated(credentials);
//
//        client.readStream("some-stream", options, observer)
//                .get();
//        // endregion overriding-user-credentials
//    }
//
//    private static void readFromStreamPositionCheck(EventStoreDBClient client) throws JsonProcessingException, InterruptedException {
//        // region checking-for-stream-presence
//        ReadStreamOptions options = ReadStreamOptions.get()
//                .forwards()
//                .fromRevision(10);
//
//        List<ResolvedEvent> events = null;
//        try {
//            client.readStream("some-stream", 20, options, observer)
//                    .get();
//        } catch (ExecutionException e) {
//            Throwable innerException = e.getCause();
//
//            if (innerException instanceof StreamNotFoundException) {
//                // ...
//            }
//        }
//        // endregion checking-for-stream-presence
//    }
//
//    private static void readFromStreamBackwards(EventStoreDBClient client) throws JsonProcessingException, ExecutionException, InterruptedException {
//        // region reading-backwards
//        ReadStreamOptions options = ReadStreamOptions.get()
//                .backwards()
//                .fromEnd();
//
//        client.readStream("some-stream", options, observer)
//                .get();
//        // endregion reading-backwards
//    }
//
//    private static void readFromAllStream(EventStoreDBClient client) throws JsonProcessingException, ExecutionException, InterruptedException {
//        // region read-from-all-stream
//        ReadAllOptions options = ReadAllOptions.get()
//                .forwards()
//                .fromStart();
//
//        client.readAll(new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//                // ...
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        })
//        .get();
//        // endregion read-from-all-stream
//
//        // region read-from-all-stream-iterate
//        client.readAll(new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//                // ...
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        })
//        .get();
//        // endregion read-from-all-stream-iterate
//    }
//
//    private static void readAllOverridingUserCredentials(EventStoreDBClient client) throws ExecutionException, InterruptedException {
//        // region read-all-overriding-user-credentials
//        UserCredentials credentials = new UserCredentials("admin", "changeit");
//
//        ReadAllOptions options = ReadAllOptions.get()
//                .forwards()
//                .fromStart()
//                .authenticated(credentials);
//
//        client.readAll(options, observer).get();
//        // endregion read-all-overriding-user-credentials
//    }
//
//    private static void ignoreSystemEvents(EventStoreDBClient client) throws JsonProcessingException, ExecutionException, InterruptedException {
//        // region ignore-system-events
//        ReadAllOptions options = ReadAllOptions.get()
//                .forwards()
//                .fromStart();
//
//        client.readAll(options, new ReadObserver<Object>() {
//                    @Override
//                    public void onNext(ResolvedEvent event) {
//                        if (!event.getOriginalEvent().getEventType().startsWith("$")) {
//                            // ...
//                        }
//                    }
//
//                    @Override
//                    public Object onCompleted() {
//                        return null;
//                    }
//
//                    @Override
//                    public void onError(Throwable error) {
//
//                    }
//                })
//       .get();
//        // endregion ignore-system-events
//    }
//
//    private static void readFromAllStreamBackwards(EventStoreDBClient client) throws JsonProcessingException, ExecutionException, InterruptedException {
//        // region read-from-all-stream-backwards
//        ReadAllOptions options = ReadAllOptions.get()
//                .backwards()
//                .fromEnd();
//
//        client.readAll(options, new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        }).get();
//        // endregion read-from-all-stream-backwards
//
//        // region read-from-all-stream-iterate
//
//        client.readAll(options, new ReadObserver<Object>() {
//            @Override
//            public void onNext(ResolvedEvent event) {
//                // Doing something...
//            }
//
//            @Override
//            public Object onCompleted() {
//                return null;
//            }
//
//            @Override
//            public void onError(Throwable error) {
//
//            }
//        }).get();
//
//        // endregion read-from-all-stream-iterate
//    }
//
//    private static void readFromStreamResolvingLinkTos(EventStoreDBClient client) throws JsonProcessingException, ExecutionException, InterruptedException {
//        // region read-from-all-stream-resolving-link-Tos
//        ReadAllOptions options = ReadAllOptions.get()
//                .forwards()
//                .fromStart()
//                .resolveLinkTos();
//
//        client.readAll(options, observer)
//                .get();
//
//        // endregion read-from-all-stream-resolving-link-Tos
//    }
//
//    private static ReadObserver<Object> observer = new ReadObserver<Object>() {
//        @Override
//        public void onNext(ResolvedEvent event) {
//        }
//
//        @Override
//        public Object onCompleted() {
//            return null;
//        }
//
//        @Override
//        public void onError(Throwable error) {
//
//        }
//    };
//}
