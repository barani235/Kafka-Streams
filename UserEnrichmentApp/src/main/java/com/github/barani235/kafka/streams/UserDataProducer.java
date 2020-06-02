package com.github.barani235.kafka.streams;

import com.github.barani235.configuration.aws.EC2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EC2.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //We do .get() to ensure the writes are sequential
        //Do not try this in Production

        //1. Create a new user then send data for the user
        producer.send(userRecord("john", "First=John, Last=Doe, Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();
        Thread.sleep(10000);

        //2. Send user purchase for non-existent user
        producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();
        Thread.sleep(10000);

        //3. Update an user and send data
        producer.send(userRecord("john", "First=Johnny, Last=Doe, Email=johnny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();
        Thread.sleep(10000);

        //4. Send data for non-existent user, then create the user, send data again, delete the user
        producer.send(purchaseRecord("steve", "Computer (4))")).get();
        producer.send(userRecord("steve", "First=Steve, Last=Smith, Ranking=1")).get();
        producer.send(purchaseRecord("steve", "Kookaburra (4))")).get();
        producer.send(userRecord("steve", null)).get();
        Thread.sleep(10000);

        //5. Create an user but delete the user before sending the data
        producer.send(userRecord("alice", "First=Alice")).get();
        producer.send(userRecord("alice", null)).get();
        producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();
        Thread.sleep(10000);

        producer.close();

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    private static ProducerRecord<String, String> userRecord(String key, String value){
        return new ProducerRecord<String, String>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value){
        return new ProducerRecord<String, String>("user-purchases", key, value);
    }

}
