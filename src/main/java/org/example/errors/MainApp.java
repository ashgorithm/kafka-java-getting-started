package org.example.errors;

import org.apache.kafka.clients.consumer.*;
import org.example.errors.events.InventoryEvent;
import org.example.errors.events.RedirectEvent;
import org.example.errors.producers.RetryProducer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Functions of the Main app are:
 * Subscribe for messages from Source Inventory Topic
 * If a message cannot be processed at that time, push it to Retry Topic
 * Also push eventId to Redirect topic
 * Update the in memory store with new event Id which is added to retry topic.
 * In memory store is used to look up what items are added to the retry topic
 * additionally it subscribes the redirect topic for any tombstone events
 * If any tombstone event is detected, it updates the in memory store
 */
public class MainApp {

    private static String sourceTopicName = "Inventory";
    private static String retryTopicName = "Retry";
    private static String redirectTopicName = "Redirect";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        RetryProducer retryProducer = new RetryProducer();
        RetryEventStore retryEventStore = new RetryEventStore();


        Consumer<String, InventoryEvent> sourceEventConsumer = getSourceEventConsumer();
        Consumer<String, RedirectEvent> redirectEventConsumer = getRedirectEventConsumer();
        while (true) {
            ConsumerRecords<String, InventoryEvent> sourceRecords = sourceEventConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, InventoryEvent> record : sourceRecords) {
                String key = record.key();
                InventoryEvent inventoryEvent = record.value();
//                totalConsumerRecords.add(record);
                String topicName = record.topic();
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topicName, key, inventoryEvent));


                if (topicName.equals(sourceTopicName)){

                    //assume iphone event cannot be processed right now.
                    //First check if related events are there in the in memory store
                    if (retryEventStore.isRelatedEventRetried(inventoryEvent.itemId) || inventoryEvent.itemName.equals("iphone")){
                        retryProducer.sendEventToRetry(inventoryEvent);
                        retryProducer.sendEventToRedirectTopic(inventoryEvent, false);
                        retryEventStore.addEvent(inventoryEvent.itemId, inventoryEvent.eventId);
                        System.out.println(retryEventStore);
                    }
                    else{
                        Processor.process(inventoryEvent);
                    }
                }
            }
            sourceEventConsumer.commitSync();

            ConsumerRecords<String, RedirectEvent> redirectRecords = redirectEventConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, RedirectEvent> record : redirectRecords) {
                String key = record.key();
                RedirectEvent redirectEvent = record.value();
//                totalConsumerRecords.add(record);
                String topicName = record.topic();
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topicName, key, redirectEvent));


                if (topicName.equals(redirectTopicName)){
                    if (redirectEvent.isTombstone){
                        retryEventStore.removeEvent(redirectEvent.eventId);
                        System.out.println(retryEventStore);
                    }
                }
            }
            redirectEventConsumer.commitSync();
        }
    }

    public static Consumer<String, InventoryEvent> getSourceEventConsumer() {
        //consume messages
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.example.errors.serde.InventoryEventSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.example.errors.serde.InventoryEventDeserializer");

        Consumer<String, InventoryEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(sourceTopicName));

        return consumer;
    }

    public static Consumer<String, RedirectEvent> getRedirectEventConsumer() {
        //consume messages
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.example.errors.serde.RedirectEventSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.example.errors.serde.RedirectEventDeserializer");

        Consumer<String, RedirectEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(redirectTopicName));

        return consumer;
    }
}
