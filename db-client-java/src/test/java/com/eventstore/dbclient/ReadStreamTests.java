package com.eventstore.dbclient;

import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import testcontainers.module.EventStoreTestDBContainer;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReadStreamTests {
    @Rule
    public final EventStoreTestDBContainer server = new EventStoreTestDBContainer(false);

    private TestSubscriber<ResolvedEvent> testSubscriber;

    @Before
    public void setUp() {
        testSubscriber = new TestSubscriber<>(0);
    }

    @Test
    public void testNoEventsAreReceivedUnlessRequested() throws Throwable {
        EventStoreDBClient client = server.getClient();

        ReadStreamOptions options = ReadStreamOptions.get()
                .forwards()
                .fromStart()
                .notResolveLinkTos();

        client.readStream("dataset20M-1800", 10, options)
                .subscribe(testSubscriber);

        Assert.assertEquals(testSubscriber.values().size(), 0);
    }

    @Test
    public void testReadStreamForward10EventsFromPositionStart() throws Throwable {
        EventStoreDBClient client = server.getClient();

        ReadStreamOptions options = ReadStreamOptions.get()
                .forwards()
                .fromStart()
                .notResolveLinkTos();

        client.readStream("dataset20M-1800", 10, options)
                .subscribe(testSubscriber);
        testSubscriber.requestMore(10);

        testSubscriber.awaitCount(10);
        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        testSubscriber.assertComplete();
        verifyAgainstTestData(testSubscriber.values(), "dataset20M-1800-e0-e10");
    }

    @Test
    public void testReadStreamBackward10EventsFromPositionEnd() throws Throwable {
        EventStoreDBClient client = server.getClient();

        ReadStreamOptions options = ReadStreamOptions.get()
                .backwards()
                .fromEnd()
                .notResolveLinkTos();

       client.readStream("dataset20M-1800", 10, options)
                .subscribe(testSubscriber);
        testSubscriber.requestMore(10);

        testSubscriber.awaitCount(10);
        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        testSubscriber.assertComplete();
        verifyAgainstTestData(testSubscriber.values(), "dataset20M-1800-e1999-e1990");
    }

    @Test
    public void testReadStreamOptionsAreNotIgnoredInOverloadedMethod() throws Throwable {
        EventStoreDBClient client = server.getClient();

        ReadStreamOptions options = ReadStreamOptions.get()
                .backwards()
                .fromEnd()
                .notResolveLinkTos();

        client.readStream("dataset20M-1800", Long.MAX_VALUE, options)
                .subscribe(testSubscriber);

        TestSubscriber<ResolvedEvent> secondTestSubscriber = new TestSubscriber<>(0);
        client.readStream("dataset20M-1800", options)
                .subscribe(secondTestSubscriber);
        testSubscriber.requestMore(Long.MAX_VALUE);
        secondTestSubscriber.requestMore(Long.MAX_VALUE);

        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        secondTestSubscriber.awaitDone(5, TimeUnit.SECONDS);
        List<ResolvedEvent> valuesOfFirstSubscription = testSubscriber.values();
        List<ResolvedEvent> valuesOfSecondSubscription = secondTestSubscriber.values();
        RecordedEvent firstEvent1 = valuesOfFirstSubscription.get(0).getOriginalEvent();
        RecordedEvent firstEvent2 = valuesOfSecondSubscription.get(0).getOriginalEvent();

        Assert.assertEquals(valuesOfFirstSubscription.size(), valuesOfSecondSubscription.size());
        Assert.assertEquals(firstEvent1.getEventId(), firstEvent2.getEventId());
    }

    private void verifyAgainstTestData(List<ResolvedEvent> actualEvents, String filenameStem) {
        ResolvedEvent[] actualEventsArray = actualEvents.toArray(new ResolvedEvent[0]);

        TestResolvedEvent[] expectedEvents = TestDataLoader.loadSerializedResolvedEvents(filenameStem);
        for (int i = 0; i < expectedEvents.length; i++) {
            TestResolvedEvent expected = expectedEvents[i];
            ResolvedEvent actual = actualEventsArray[i];

            expected.assertEquals(actual);
        }
    }
}
