package org.example.errors.producers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.errors.events.InventoryEvent;
import org.example.errors.events.RedirectEvent;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Producer used to send events to Redirect topic
 * whenever a event is added to retry topic, eventId is pushed to redirect topic to keep track
 * Whenever a event is processed by Retry App, tombstone eventid is pushed to redirect topic
 */
public class RetryProducer {
    private KafkaProducer<String, InventoryEvent> retryProducer;

    private KafkaProducer<String, RedirectEvent> redirectProducer;
    private String retryTopicName = "Retry";
    private String redirectTopicName = "Redirect";

    private int key = 1;

    public RetryProducer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.example.errors.serde.InventoryEventSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.example.errors.serde.InventoryEventDeserializer");
        retryProducer = new KafkaProducer<>(props);

        Properties redirectProducerProps = new Properties(props);
        redirectProducerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        redirectProducerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        redirectProducerProps.put("bootstrap.servers", "localhost:9092");
        redirectProducerProps.put("acks", "1");
        redirectProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        redirectProducerProps.put("value.serializer", "org.example.errors.serde.RedirectEventSerializer");
        redirectProducerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        redirectProducerProps.put("value.deserializer", "org.example.errors.serde.RedirectEventDeserializer");
        redirectProducer = new KafkaProducer<>(redirectProducerProps);
        int key = 1;
    }

    public void sendEventToRetry(InventoryEvent inventoryEvent) throws ExecutionException, InterruptedException {

        String key = String.valueOf(inventoryEvent.eventId);

        Future<RecordMetadata> future = retryProducer.send(new ProducerRecord<String, InventoryEvent>(retryTopicName, key, inventoryEvent),
                (event, ex) -> {
                    if (ex != null)
                        System.out.println("exception for record " + inventoryEvent);
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", retryTopicName, key, inventoryEvent);
                });

        future.get();
    }

    public void sendEventToRedirectTopic(InventoryEvent inventoryEvent, boolean isTombstone) throws ExecutionException, InterruptedException {
        String key = String.valueOf(inventoryEvent.eventId);
        RedirectEvent redirectEvent = new RedirectEvent();
        redirectEvent.eventId = inventoryEvent.eventId;
        redirectEvent.isTombstone = isTombstone;

        Future<RecordMetadata> future = redirectProducer.send(new ProducerRecord<String, RedirectEvent>(redirectTopicName, key, redirectEvent),
                (event, ex) -> {
                    if (ex != null)
                        System.out.println("exception for record " + inventoryEvent);
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", redirectTopicName, key, redirectEvent);
                });

        future.get();
    }
}
