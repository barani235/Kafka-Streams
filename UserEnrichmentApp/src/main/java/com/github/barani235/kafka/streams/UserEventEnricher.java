package com.github.barani235.kafka.streams;

import com.github.barani235.configuration.aws.EC2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventEnricher {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, EC2.bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"user-enrichment");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> userTable = builder.globalTable("user-table");

        KStream<String, String> userPurchases = builder.stream("user-purchases");

        KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(
                userTable,
                (key,value) -> key,
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + ", User Info[" + userInfo +"]"
        );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(
                userTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> {
                    if (userInfo != null) {
                        return "Purchase=" + userPurchase + ", User Info[" + userInfo + "]";
                    }
                    else {
                        return "Purchase=" + userPurchase + ", User Info=null";
                    }
                }
        );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp();

        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
