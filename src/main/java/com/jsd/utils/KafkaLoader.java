package com.jsd.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.json.JSONObject;

import java.io.FileInputStream;
import java.util.Properties;

public class KafkaLoader {

    private Properties config;
    private RandomDataGenerator gen;

    public KafkaLoader(String configFile) throws Exception {
        config = new Properties();
        config.load(new FileInputStream(configFile));
        gen = new RandomDataGenerator(config.getProperty("data.template.file"));
    }

    public void streamData() throws Exception {
        // Kafka broker address
        String broker = config.getProperty("broker.host", "localhost:9092");

        // Create Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Topic name
        String topic = config.getProperty("topic.name");

        int batchSize = Integer.parseInt(config.getProperty("batch.size", "500"));
        int numBatches = Integer.parseInt(config.getProperty("num.batches", "1000"));

        JSONObject record = null;

        // Add events to the topic
        for (int b = 0; b < numBatches; b++) {
            long timestamp = System.currentTimeMillis();

            for (int i = 0; i < batchSize; i++) {

                record = gen.generateRecord("header");
                
                ProducerRecord<String, String> event = new ProducerRecord<>(topic, String.valueOf(timestamp + (long)i), record.toString());
                producer.send(event);

            }

            Thread.sleep(1000l);
        }

        System.out.println("[KafkaLoader] Streamed " + (batchSize * numBatches) + " Records");

        producer.close();
    }

    public static void main(String[] args) throws Exception {
        KafkaLoader kl = new KafkaLoader("./kafka-config.properties");
        kl.streamData();
    }
}
