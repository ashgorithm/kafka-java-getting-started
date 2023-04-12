package org.example.errors;

import org.apache.kafka.clients.consumer.*;
import org.example.errors.events.InventoryEvent;
import org.example.errors.producers.RetryProducer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Functions of the Retry App
 * Subscribes to the Retry topic
 * Processes the event by periodically retrying
 * After successfully processing the event places it in the Target topic
 * Pushes a tombstone event to the Redirect topic
 */

public class RetryApp {

    private static final String retryTopicName = "Retry";


    public static void main(String[] args) throws ExecutionException, InterruptedException {


        RetryProducer retryProducer = new RetryProducer();

        Consumer<String, InventoryEvent> consumer = getConsumer();


        while (true) {
            ConsumerRecords<String, InventoryEvent> retryRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, InventoryEvent> record : retryRecords) {
                String key = record.key();
                InventoryEvent inventoryEvent = record.value();
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", retryTopicName, key, inventoryEvent));


                //Process the event based on some current inventory count availability. availability check is skipped here.
                Processor.process(inventoryEvent);

                //publish a tombstone event after processing
                retryProducer.sendEventToRedirectTopic(inventoryEvent, true);

            }
            consumer.commitSync();
        }
    }

    public static Consumer<String, InventoryEvent> getConsumer(){
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

        //Subscribe to the Retry topic
        consumer.subscribe(Arrays.asList(retryTopicName));
        return consumer;
    }
}
