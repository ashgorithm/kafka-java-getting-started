package org.example.errors.producers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.errors.events.InventoryEvent;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Sample producer to simulate the source topic events
 * Events are related to a product inventory
 * We have used two products iphone and samsung TV
 *
 */
public class InventoryProducer {
    private KafkaProducer<String, InventoryEvent> kafkaProducer;
    private String sourceTopicName = "Inventory";
    private String retryTopicName = "Retry";
    private String redirectTopicName = "Redirect";

    private int key = 1;

    public InventoryProducer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.example.errors.serde.InventoryEventSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.example.errors.serde.InventoryEventDeserializer");
        kafkaProducer = new KafkaProducer<>(props);
        int key = 1;
    }

    public void sendEvent(InventoryEvent inventoryEvent) throws ExecutionException, InterruptedException {

        key++;
        Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<String, InventoryEvent>(sourceTopicName, String.valueOf(key), inventoryEvent),
                (event, ex) -> {
                    if (ex != null)
                        System.out.println("exception for record " + inventoryEvent);
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", sourceTopicName, key, inventoryEvent);
                });

        future.get();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        InventoryProducer inventoryProducer = new InventoryProducer();
        String Products[] = {"iphone", "samsung TV"};

        for(int i = 0; i < 10; i++)
        {
            InventoryEvent inventoryEvent = new InventoryEvent();
            inventoryEvent.eventId = i;
            inventoryEvent.itemId = i % 2;
            inventoryEvent.itemName = Products[i % 2];
            inventoryEvent.change = 10;

            inventoryProducer.sendEvent(inventoryEvent);
        }
    }
}
