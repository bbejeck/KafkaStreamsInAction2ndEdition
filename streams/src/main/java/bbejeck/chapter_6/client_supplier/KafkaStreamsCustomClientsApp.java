/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbejeck.chapter_6.client_supplier;

import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsCustomClientsApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsCustomClientsApp.class);

    public Topology topology() {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("src-topic",
                Consumed.with(stringSerde, stringSerde))
                .mapValues((key, value) -> value.toUpperCase())
                .to("out-topic",
                        Produced.with(stringSerde, stringSerde));
       return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Properties properties = getProperties();
        LOG.info("Creating the required topics");
        Topics.maybeDeleteThenCreate("src-topic", "out-topic");
        KafkaStreamsCustomClientsApp streamsApplication = new KafkaStreamsCustomClientsApp();
        Topology topology = streamsApplication.topology();
        try(KafkaStreams kafkaStreams = new KafkaStreams(topology,
                                                     properties,
                                                     new CustomKafkaStreamsClientSupplier())) {
            
           LOG.debug("Starting the Kafka Streams application");
           kafkaStreams.start();
           Thread.sleep(30000);
        }

    }

    @NotNull
    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return properties;
    }
}