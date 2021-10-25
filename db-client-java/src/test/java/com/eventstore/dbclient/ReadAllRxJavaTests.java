package com.eventstore.dbclient;

import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testcontainers.module.EventStoreTestDBContainer;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ReadAllRxJavaTests {

    private static final Logger logger = LoggerFactory.getLogger(ReadAllRxJavaTests.class);

    @Rule
    public final EventStoreTestDBContainer server = new EventStoreTestDBContainer(false);

    @Test
    public void testReadAllEventsForwardFromZeroPosition() throws Exception {
        EventStoreDBClient client = server.getClient();

        ReadAllOptions options = ReadAllOptions.get()
                .forwards()
                .fromStart()
                .notResolveLinkTos();

        Flowable<ResolvedEvent> eventFlow = Flowable.fromPublisher(new EventPublisher(client, options, 1000));
        List<ResolvedEvent> result = new LinkedList<>();
        eventFlow
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.io())
                .doOnNext(e -> logger.info("{}: Received event.", Instant.now()))
                .doOnNext(e -> {
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                    result.add(e);
                })
                .blockingLast();

        assertEquals(1000, result.size());
        verifyAgainstTestData(result, "all-e0-e10");
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
