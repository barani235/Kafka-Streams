package com.github.barani235.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionsProducerTests {

    @Test
    public void newRandomTransactionTests(){
        ProducerRecord<String, String> record = BankTransactionsProducer.newRandomTransaction("barani");
        String key = record.key();
        String value = record.value();

        assertEquals(key,"barani");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            assertTrue("Amount should be between 0 and 100", node.get("amount").asInt() > 0 && node.get("amount").asInt() < 100);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        System.out.println(value);
    }

}
