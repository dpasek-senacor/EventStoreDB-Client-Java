package com.eventstore.dbclient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.TestEnvironment;
import testcontainers.module.EventStoreTestDBContainer;

public class EventPublisherVerificationTest extends PublisherVerificationJUnit<ResolvedEvent> {

    @Rule
    public final EventStoreTestDBContainer server = new EventStoreTestDBContainer(false);

    private EventStoreDBClient client;

    public EventPublisherVerificationTest() {
        super(new TestEnvironment(2000, 500, true));
    }

    @Before
    public void setUp() {
        client = server.getClient();
    }

    @After
    public void shutdown() throws Exception {
        client.shutdown();
    }

    @Override
    public Publisher<ResolvedEvent> createPublisher(long elements) {

        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart()
                .notResolveLinkTos();

        return new EventPublisher(client, options, elements);
    }

    @Override
    public Publisher<ResolvedEvent> createFailedPublisher() {
        return null;
    }
}
