package com.eventstore.dbclient;

import io.reactivex.subscribers.TestSubscriber;
import org.junit.Rule;
import org.junit.Test;
import testcontainers.module.EventStoreStreamsClient;
import testcontainers.module.EventStoreTestDBContainer;

import java.util.Objects;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

class Foo {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Foo foo1 = (Foo) o;
        return foo == foo1.foo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(foo);
    }

    private boolean foo;

    public boolean isFoo() {
        return foo;
    }

    public void setFoo(boolean foo) {
        this.foo = foo;
    }
}

public class AppendTests {
    @Rule
    public final EventStoreTestDBContainer server = new EventStoreTestDBContainer(true);

    @Rule
    public final EventStoreStreamsClient client = new EventStoreStreamsClient(server);

    @Test
    public void testAppendSingleEventNoStream() throws Throwable {
        EventStoreDBClient client = server.getClient();

        final String streamName = "testIntegrationAppendSingleEventNoStream";
        final String eventType = "TestEvent";
        final String eventId = "38fffbc2-339e-11ea-8c7b-784f43837872";
        final byte[] eventMetaData = new byte[]{0xd, 0xe, 0xa, 0xd};

        EventData event = EventData.builderAsJson(eventType, new Foo())
                .metadataAsBytes(eventMetaData)
                .eventId(UUID.fromString(eventId))
                .build();

        AppendToStreamOptions appendOptions = AppendToStreamOptions.get()
                .expectedRevision(ExpectedRevision.NO_STREAM);

        WriteResult appendResult = client.appendToStream(streamName, appendOptions, event)
                .get();

        assertEquals(new StreamRevision(0), appendResult.getNextExpectedRevision());

        ReadStreamOptions readStreamOptions = ReadStreamOptions.get()
                .fromEnd()
                .backwards();

        // Ensure appended event is readable
        TestSubscriber<ResolvedEvent> testSubscriber = new TestSubscriber<>();
        client.readStream(streamName, 1, readStreamOptions)
                .subscribe(testSubscriber);

        assertEquals(1, testSubscriber.values().size());
        RecordedEvent first = testSubscriber.values().get(0).getEvent();

        assertEquals(streamName, first.getStreamId());
        assertEquals(eventType, first.getEventType());
        assertEquals(eventId, first.getEventId().toString());
        assertArrayEquals(eventMetaData, first.getUserMetadata());
        assertEquals(new Foo(), first.getEventDataAs(Foo.class));
    }
}
