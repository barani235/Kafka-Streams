package com.github.barani235.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class BankBalanceStreamTests {
    @Test
    public void newBalanceTests(){
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode balance = mapper.readTree("{\"count\":\"0\",\"balance\":\"0\",\"time\":\"2020-05-31T19:22:31.323Z\"}");
            JsonNode transaction = mapper.readTree("{\"name\":\"john\",\"amount\":\"85\",\"now\":\"2020-05-31T19:24:31.323Z\"}");
            JsonNode newBalance = BankBalanceStream.newBalance(transaction, balance);
            System.out.println(newBalance.toString());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
