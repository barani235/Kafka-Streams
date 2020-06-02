package com.github.barani235.kafka.streams;

import com.github.barani235.configuration.aws.EC2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, EC2.bootstrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //1.Stream from Kafka : <null,"Kafka Kafka Streams">
        KStream<String,String> wordCountInput = builder.stream("word-count-input");

        KTable<String,Long> wordCounts = wordCountInput
                //2.Map values to lowercase : <null,"kafka kafka streams">
                .mapValues(value -> value.toLowerCase())
                //3.FlatMap values split by space : <null,"kafka">,<null,"kafka">,<null,"streams">
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                //4.SelectKey to apply a key : <"kafka","kafka">,<"kafka","kafka">,<"streams","streams">
                .selectKey((key,value) -> value)
                //5.GroupByKey before aggregation : (<"kafka","kafka">,<"kafka","kafka">),<"streams","streams">
                .groupByKey()
                //6.Count occurrences : <"kafka",2>,<"streams",1>
                .count();
        //7.To in order to write results back to Kafka : push the messages back to a Kafka topic
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(),properties);
        streams.start();

        System.out.println(streams.toString());

        //Gracefully shutdown the application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
