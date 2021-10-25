package com.eventstore.dbclient;

import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import testcontainers.module.EventStoreTestDBContainer;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReadAllTests {
    @Rule
    public final EventStoreTestDBContainer server = new EventStoreTestDBContainer(false);

    private TestSubscriber<ResolvedEvent> testSubscriber;

    @Before
    public void setUp() {
        testSubscriber = new TestSubscriber<>(10);
    }

    @Test
    public void testReadAllEventsForwardFromZeroPosition() {
        EventStoreDBClient client = server.getClient();

        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart()
                .notResolveLinkTos();

        client.readAll(10, options).subscribe(testSubscriber);

        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        testSubscriber.assertComplete();
        verifyAgainstTestData(testSubscriber.values(), "all-e0-e10");
    }

    @Test
    public void testReadAllEventsForwardFromNonZeroPosition() {
        EventStoreDBClient client = server.getClient();

        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromPosition(new Position(1788, 1788))
                .notResolveLinkTos();

        client.readAll(10, options).subscribe(testSubscriber);

        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        testSubscriber.assertComplete();
        verifyAgainstTestData(testSubscriber.values(), "all-c1788-p1788");
    }

    @Test
    public void testReadAllEventsBackwardsFromZeroPosition() {
        EventStoreDBClient client = server.getClient();

        ReadAllOptions options = ReadAllOptions.get()
                .backwards()
                .fromEnd()
                .notResolveLinkTos();

        client.readAll(10, options).subscribe(testSubscriber);

        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        testSubscriber.assertComplete();
        verifyAgainstTestData(testSubscriber.values(), "all-back-e0-e10");
    }

    @Test
    public void testReadAllEventsBackwardsFromNonZeroPosition() {
        EventStoreDBClient client = server.getClient();

        ReadAllOptions options = ReadAllOptions.get()
                .backwards()
                .fromPosition(new Position(3386, 3386))
                .notResolveLinkTos();

        client.readAll(10, options).subscribe(testSubscriber);

        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        testSubscriber.assertComplete();
        verifyAgainstTestData(testSubscriber.values(), "all-back-c3386-p3386");
    }

    @Test
    public void testNoEventsAreReceivedUnlessRequested() {
        testSubscriber = new TestSubscriber<>(0);
        EventStoreDBClient client = server.getClient();

        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart()
                .notResolveLinkTos();

        client.readAll(10, options).subscribe(testSubscriber);

        Assert.assertEquals(testSubscriber.values().size(), 0);
    }

    @Test
    public void testMoreEventsCanBeRequested() {
        EventStoreDBClient client = server.getClient();
        testSubscriber = new TestSubscriber<>(2);

        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart()
                .notResolveLinkTos();

        client.readAll(10, options).subscribe(testSubscriber);
        testSubscriber.awaitCount(2);
        testSubscriber.assertNotComplete();

        testSubscriber.requestMore(10);
        testSubscriber.awaitDone(5, TimeUnit.SECONDS);
        testSubscriber.assertComplete();
        verifyAgainstTestData(testSubscriber.values(), "all-e0-e10");
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
