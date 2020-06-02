package com.github.barani235.kafka.streams;

import com.github.barani235.configuration.aws.EC2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Arrays;
import java.util.Properties;


public class FavouriteColor {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, EC2.bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"Favourite-Color");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> inputStream = builder.stream("favourite-color-input");

        inputStream.filter((key,value) -> value.contains(","))
                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((key,value) -> Arrays.asList("red","blue","green").contains(value))
                .to("intermediate");

        KTable<String,String> table = builder.table("intermediate");

        table.groupBy((key,value) -> KeyValue.pair(value,"1"))
                .count()
                .toStream()
                .to("favourite-color-output", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(),properties);

        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
