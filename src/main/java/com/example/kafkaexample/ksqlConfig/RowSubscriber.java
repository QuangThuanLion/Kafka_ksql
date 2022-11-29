package com.example.kafkaexample.ksqlConfig;

import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class RowSubscriber implements Subscriber<Row> {

    private Subscription subscription;

    public RowSubscriber() {

    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        System.out.println("Subscriber is subscribed");
        this.subscription = subscription;

        subscription.request(1);
    }

    @Override
    public synchronized void onNext(Row row) {
        System.out.println("Received a row");
        System.out.println("Row: " + row.values());

        subscription.request(1);
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        System.out.println("Received an error: " + throwable);
    }

    @Override
    public synchronized void onComplete() {
        System.out.println("Query has ended. ");
    }
}
