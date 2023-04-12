package org.example.errors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.errors.events.InventoryEvent;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Processes the event
 * pushes the result onto target topic
 */
public class Processor {
    private static final String targetTopic = "Target";
    public static void process(InventoryEvent inventoryEvent) throws InterruptedException, ExecutionException {
        Producer<String, String> producer = getTargetProducer();
        String key = String.valueOf(inventoryEvent.eventId);

        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(targetTopic, String.valueOf(inventoryEvent.eventId), String.valueOf(inventoryEvent.eventId)),
                (event, ex) -> {
                    if (ex != null)
                        System.out.println("exception for record " + key);
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", targetTopic, key, key);
                });

        future.get();

        System.out.println("processed inventory event: " + inventoryEvent.eventId);
        Thread.sleep(2000);

    }

    public static Producer<String, String> getTargetProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Producer<String, String> targetProducer = new KafkaProducer<>(props);
        return targetProducer;

    }
}
