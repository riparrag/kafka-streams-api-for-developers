package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;

public class GreetingsTopology {
    public static String SOURCE_TOPIC = "greetings";
    public static String DESTINATION_TOPIC = "greetings_uppercase";

    public static Topology buildTopology() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> greetingsStream = streamsBuilder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        greetingsStream.print(Printed.<String,String>toSysOut().withLabel("greetingsStream"));

        KStream<String, String> modifiedStream = greetingsStream.filterNot(GreetingsTopology::valueIsNotAllowed)
                                                                .flatMap((k,v)-> {
                                                                    return Arrays.stream(v.split(""))
                                                                                 .map(s ->KeyValue.pair(k.toUpperCase(), s.toUpperCase()))
                                                                                 .toList();
                                                                });

        modifiedStream.to(DESTINATION_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        modifiedStream.print(Printed.<String,String>toSysOut().withLabel("modifiedGreetingsStream"));

        return streamsBuilder.build();
    }

    private static boolean valueIsNotAllowed(String key, String value) {
        return List.of("hola", "chau", "si", "no").contains(value);
    }

    public static Topology buildTopologyRoWay() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                      .mapValues(((readOnlyKey, value) -> value.toUpperCase() ))
                      .to(DESTINATION_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
