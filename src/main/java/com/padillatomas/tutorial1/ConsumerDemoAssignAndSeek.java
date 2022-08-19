package com.padillatomas.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        final String bootstrapServers = "127.0.0.1:9092";
        final String topic1 = "first_topic";

//        Configs:
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        Create Consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);

// Assign and Seek are used to replay data or fetch a specific message:
        int numberOfMessagesToRead = 5;
        boolean keepReading = true;
        int messagesReadSoFar = 0;

        // Assign:
        long offsetsToReadFrom = 15L;
        TopicPartition toReadFrom = new TopicPartition(topic1, 0);
        consumer.assign(Arrays.asList(toReadFrom));

        // Seek:
        consumer.seek(toReadFrom, offsetsToReadFrom);

//        POLL for new Data:
        while (keepReading) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offests: " + record.offset());
                if (messagesReadSoFar >= numberOfMessagesToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("EXITING APP");


    }
}
