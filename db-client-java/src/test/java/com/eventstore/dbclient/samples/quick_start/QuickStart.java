package com.eventstore.dbclient.samples.quick_start;

import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.EventStoreDBConnectionString;
import com.eventstore.dbclient.ParseError;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.UserCredentials;
import com.eventstore.dbclient.samples.TestEvent;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class QuickStart {
    public static void Run() throws ParseError, ExecutionException, InterruptedException {
        // region createClient
        EventStoreDBClientSettings settings = EventStoreDBConnectionString.parse("{connectionString}");
        EventStoreDBClient client = EventStoreDBClient.create(settings);
        // endregion createClient

        // region createEvent
        TestEvent event = new TestEvent();
        event.setId(UUID.randomUUID().toString());
        event.setImportantData("I wrote my first event!");

        EventData eventData = EventData
                .builderAsJson("TestEvent", event)
                .build();
        // endregion createEvent

        // region appendEvents
        client.appendToStream("some-stream", eventData)
                .get();
        // endregion appendEvents

        // region overriding-user-credentials
        AppendToStreamOptions appendToStreamOptions = AppendToStreamOptions.get()
                .authenticated(new UserCredentials("admin", "changeit"));

        client.appendToStream("some-stream", appendToStreamOptions, eventData)
                .get();
        // endregion overriding-user-credentials


        // region readStream
        ReadStreamOptions options = ReadStreamOptions.get()
                .forwards()
                .fromStart();

        client.readStream("some-stream", 10, options).subscribe(new Subscriber<ResolvedEvent>() {

            @Override
            public void onSubscribe(Subscription s) {
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
        // endregion readStream
    }
}
