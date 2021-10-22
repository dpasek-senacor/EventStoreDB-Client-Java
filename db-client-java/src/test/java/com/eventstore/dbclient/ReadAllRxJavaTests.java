package com.eventstore.dbclient;

import com.google.common.util.concurrent.Uninterruptibles;
import io.perfmark.PerfMark;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testcontainers.module.EventStoreTestDBContainer;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
        PerfMark.setEnabled(true);
        eventFlow
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.io())
                .doOnNext(e -> logger.info("{}: Received event.", Instant.now()))
                .doOnNext(e -> {
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                    result.add(e);
                })
                .blockingLast();
        PerfMark.setEnabled(false);

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

    private static class EventPublisher implements Publisher<ResolvedEvent> {
        private final EventStoreDBClient client;
        private final ReadAllOptions options;
        private final int maxCount;

        private EventPublisher(EventStoreDBClient client, ReadAllOptions options, int maxCount) {
            this.client = client;
            this.options = options;
            this.maxCount = maxCount;
        }

        @Override
        public void subscribe(Subscriber<? super ResolvedEvent> subscriber) {
            PublisherSubscription subscription = new PublisherSubscription(subscriber);
            subscriber.onSubscribe(subscription);

            client.readAll(maxCount, options, new ReadObserver<Object>() {
                @Override
                public void onNext(ResolvedEvent event) {
                    subscription.onNext(event);
                }

                @Override
                public Object onCompleted() {
                    subscription.onCompleted();
                    return null;
                }

                @Override
                public void onError(Throwable error) {
                    subscription.cancel();
                    subscriber.onError(error);
                }
            });
        }

        private static class PublisherSubscription implements Subscription {
            private final Subscriber<? super ResolvedEvent> subscriber;
            private final AtomicLong requested = new AtomicLong(0);
            private final AtomicBoolean cancelled = new AtomicBoolean(false);
            private final Lock lock = new ReentrantLock();
            private final Condition hasRequested = lock.newCondition();

            private PublisherSubscription(Subscriber<? super ResolvedEvent> subscriber) {
                this.subscriber = subscriber;
            }

            private void onNext(ResolvedEvent event) {
                lock.lock();
                while (requested.get() == 0 && !cancelled.get()) {
                    hasRequested.awaitUninterruptibly();
                }
                if (!cancelled.get()) {
                    subscriber.onNext(event);
                    requested.decrementAndGet();
                }
                lock.unlock();
            }

            private void onCompleted() {
                subscriber.onComplete();
            }

            @Override
            public void request(long n) {
                lock.lock();
                requested.updateAndGet(current -> current + n);
                hasRequested.signal();
                lock.unlock();
            }

            @Override
            public void cancel() {
                cancelled.compareAndSet(false, true);
            }
        }
    }
}
