package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ProducerConsumerTest {

    /*
    Create 5 topics

    create 5 producers
    send mock data to the topics

    create consumers
    poll for data
    print and verify

    end



     */

    public static void main(String[] args) {
        ProducerConsumerTest producerConsumerTest = new ProducerConsumerTest();
        producerConsumerTest.run();
    }

    public ProducerConsumerTest(){
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
    Properties properties = new Properties();
    public String users[] = {"jsmith", "afinch", "sdube", "mali", "dconvoy"};
    public String userDetails[] = {"Jason Smith", "Aaron Finch", "Shivam Dube", "Moin Ali", "Devon Convoy"};
    String purchases[] = {"books", "iphone", "plant", "sofa", "watch"};
    String pageViews[] = {"3","6","2","6","9"};

    String salaries[] = {"60K", "90K", "100K", "120K", "200K"};

    String steps[] = {"7K", "8K", "9K", "10K", "12K"};


    public void run() {

        producePurchases();
        consumePurchases();
//        produceUsers(properties);
//        consumeUsers(properties);
        producePageViews();
        consumePageViews();
        produceSalaries();
        consumeSalaries();
        produceSteps();
        consumeSteps();
    }


    public void producePurchases() {
        String topicName = "purchases";
        KafkaProducer producer = new KafkaProducer(properties);

        for (int i = 0; i < 5; i++) {
            final String user = users[i];
            final String purchase = purchases[i];
            producer.send(
                    new ProducerRecord<>(topicName, user, purchase),
                    (event, ex) -> {
                        if (ex != null)
                            System.out.println("exception for record " + user);
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topicName, user, purchase);
                    });;
        }
    }

    public void produceUsers() {
        String topicName = "userDetails";
        KafkaProducer producer = new KafkaProducer(properties);

        for (int i = 0; i < 5; i++) {
            final String user = users[i];
            final String userDetail = userDetails[i];
            producer.send(
                    new ProducerRecord<>(topicName, user, userDetail),
                    (event, ex) -> {
                        if (ex != null)
                            System.out.println("exception for record " + user);
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topicName, user, userDetail);
                    });;
        }
    }

    public void producePageViews() {
        String topicName = "pageViews";
        KafkaProducer producer = new KafkaProducer(properties);

        for (int i = 0; i < 5; i++) {
            final String user = users[i];
            final String pageView = pageViews[i];
            producer.send(
                    new ProducerRecord<>(topicName, user, pageView),
                    (event, ex) -> {
                        if (ex != null)
                            System.out.println("exception for record " + user);
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topicName, user, pageView);
                    });;
        }
    }

    public void produceSalaries() {
        String topicName = "salaries";
        KafkaProducer producer = new KafkaProducer(properties);

        for (int i = 0; i < 5; i++) {
            final String user = users[i];
            final String salary = salaries[i];
            producer.send(
                    new ProducerRecord<>(topicName, user, salary),
                    (event, ex) -> {
                        if (ex != null)
                            System.out.println("exception for record " + user);
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topicName, user, salary);
                    });;
        }
    }

    public void produceSteps() {
        String topicName = "steps";
        KafkaProducer producer = new KafkaProducer(properties);

        for (int i = 0; i < 5; i++) {
            final String user = users[i];
            final String step = steps[i];
            producer.send(
                    new ProducerRecord<>(topicName, user, step),
                    (event, ex) -> {
                        if (ex != null)
                            System.out.println("exception for record " + user);
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topicName, user, step);
                    });;
        }
    }

    public void consumePurchases() {
        String topicName = "purchases";
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        List<ConsumerRecord> totalConsumerRecords = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                totalConsumerRecords.add(record);
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topicName, key, value));
            }
            if(totalConsumerRecords.size() >= purchases.length){
                break;
            }
        }
        consumer.commitSync();
        consumer.close();
    }

    public void consumeUsers() {
        String topicName = "userDetails";
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        List<ConsumerRecord> totalConsumerRecords = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                totalConsumerRecords.add(record);
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topicName, key, value));
            }
            if(totalConsumerRecords.size() >= purchases.length){
                break;
            }
        }
        consumer.commitSync();
        consumer.close();
    }

    public void consumePageViews() {
        String topicName = "pageViews";
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        List<ConsumerRecord> totalConsumerRecords = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                totalConsumerRecords.add(record);
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topicName, key, value));
            }
            if(totalConsumerRecords.size() >= purchases.length){
                break;
            }
        }
        consumer.commitSync();
        consumer.close();
    }

    public void consumeSalaries() {
        String topicName = "salaries";
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        List<ConsumerRecord> totalConsumerRecords = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                totalConsumerRecords.add(record);
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topicName, key, value));
            }
            if(totalConsumerRecords.size() >= purchases.length){
                break;
            }
        }
        consumer.commitSync();
        consumer.close();
    }

    public void consumeSteps() {
        String topicName = "steps";
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        List<ConsumerRecord> totalConsumerRecords = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                totalConsumerRecords.add(record);
                System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topicName, key, value));
            }
            if(totalConsumerRecords.size() >= purchases.length){
                break;
            }
        }
        consumer.commitSync();
        consumer.close();
    }
}
