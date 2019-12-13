package com.chargebee.hackathon.KafkaStreamerTemplate.stream;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaStreamer {

	@Value("${kafka.server.url}") private String kafkaServerUrl;

	@Value("${kafka.stream.group.id}") private String kafkaStreamApplicationId;

	@Value("${kafka.stream.thread.max}") private int kafkaStreamThreads;

	@Value("${kafka.stripe.jobs.topic}") private String stripeKafkaJobTopic;

	@Value("${kafka.stripe.response.topic}") private String stripeKafkaResponseTopic;
	
	public void start() {
		
Properties props = new Properties();
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamApplicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerUrl);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaStreamThreads);
		
		StreamsConfig config = new StreamsConfig(props);

		StreamsBuilder builder = new StreamsBuilder();

		KStream<Object, Object> stream = builder.stream(stripeKafkaJobTopic);
		
		KStream<Object, Object> outputStream = stream.mapValues(new ValueMapper<Object, Object>() {

			@Override
			public String apply(Object value) {
				
				//TODO: Implement your logic here for each received value here.
				return null;
				
			}
		});
		outputStream.to(stripeKafkaResponseTopic);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
		
	}

}
