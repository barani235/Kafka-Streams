package com.github.barani235.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.barani235.configuration.aws.EC2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EC2.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Properties to achieve exactly once capabilities
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); //strongest producing guarantee
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        //Leverage idempotent producer from Kafka 0.11 onwards!
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //Ensures we don't push duplicates

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        int i=0;
        while(true){
            System.out.println("Producing batch " + i);
            try {
                producer.send(newRandomTransaction("stephane"));
                Thread.sleep(999);
                producer.send(newRandomTransaction("alice"));
                Thread.sleep(999);
                producer.send(newRandomTransaction("john"));
                Thread.sleep(999);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name){

        //Create an empty JSON object
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        //Create a random integer between 0 and 100 excluded
        Integer amount = ThreadLocalRandom.current().nextInt(0,100);

        //Get current time
        Instant now = Instant.now();

        //Write data to the JSON document
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("now", now.toString());

        //Return a Producer Record with name as key and the JSON document as value
        return new ProducerRecord<String, String>("bank-balance-input", name, transaction.toString());

    }
}
