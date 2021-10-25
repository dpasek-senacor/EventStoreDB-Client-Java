package com.eventstore.dbclient;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class EventPublisher implements Publisher<ResolvedEvent> {

    private final EventStoreDBClient client;
    private final ReadAllOptions options;
    private final long maxCount;

    EventPublisher(EventStoreDBClient client, ReadAllOptions options, long maxCount) {
        this.client = client;
        this.options = options;
        this.maxCount = maxCount;
    }

    @Override
    public void subscribe(Subscriber<? super ResolvedEvent> subscriber) {
        PublisherSubscription subscription = new PublisherSubscription(subscriber);
        subscriber.onSubscribe(subscription);
        log("Successfully subscribed.");

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
        log("Successfully started streaming events.");
    }

    private static class PublisherSubscription implements Subscription {
        private final Subscriber<? super ResolvedEvent> subscriber;
        private final AtomicLong requested = new AtomicLong(0);
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final Lock lock = new ReentrantLock();
        private final Condition hasRequested = lock.newCondition();

        private PublisherSubscription(Subscriber<? super ResolvedEvent> subscriber) {
            this.subscriber = subscriber;
        }

        private void onNext(ResolvedEvent event) {
            log("onNext");
            lock.lock();
            while (requested.get() == 0 && !terminated.get()) {
                hasRequested.awaitUninterruptibly();
            }
            if (!terminated.get()) {
                subscriber.onNext(event);
                requested.decrementAndGet();
                log("Forwarded next event to subscriber. [" + requested.get() + "]");
            }
            lock.unlock();
        }

        private void onCompleted() {
            log("Completed");
            if (!terminated.get()) {
                subscriber.onComplete();
            }
            terminated.compareAndSet(false, true);
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                subscriber.onError(new IllegalArgumentException("Request must be positive long value: " + n));
            }
            lock.lock();
            requested.updateAndGet(current -> current + n);
            log("Requested [" + n + "] elements. Total requested: " + requested.get());
            hasRequested.signal();
            lock.unlock();
        }

        @Override
        public void cancel() {
            log("Cancelled");
            terminated.compareAndSet(false, true);
        }

    }

    private static void log(String msg) {
        System.out.println(DateTimeFormatter.ISO_INSTANT.format(Instant.now()) + " EventPublisher [" + Thread.currentThread().getName() + "]: " + msg);
    }
}
