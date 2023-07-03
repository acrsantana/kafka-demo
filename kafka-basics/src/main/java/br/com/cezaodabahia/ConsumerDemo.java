package br.com.cezaodabahia;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Kafka consumer");

        String groupId = "java-app";
        String offset = "earliest";
        String topic = "demo_java";

        // Criando as propriedades do consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", offset);

        // Criando o consumer - os generics correspondem aos tipos dos serializers

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){
            // Assinando um tópico
            consumer.subscribe(List.of(topic));

            // Lendo dados do tópico

            while (true){
                log.info("Polling...");
                ConsumerRecords<String, String> records = consumer.poll(3000);

                for (ConsumerRecord<String, String> record: records){
                    log.info("Key: " + record.key() + " | Value: " + record.value());
                    log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                }
            }
        }

    }
}