package com.eventstore.dbclient;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

public class EventCollectorReadObserver implements Subscriber<ResolvedEvent> {
    List<ResolvedEvent> events;
    Subscription subscription;


    @Override
    public void onSubscribe(Subscription s) {
        events = new ArrayList<>();
        this.subscription = s;
    }

    @Override
    public void onNext(ResolvedEvent event) {
        events.add(event);
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void onError(Throwable error) {

    }
}
