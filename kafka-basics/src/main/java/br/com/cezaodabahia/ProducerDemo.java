package br.com.cezaodabahia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Kafka producer");

        // Criando as propriedades do producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // Criando o producer - os generics correspondem aos tipos dos serializers
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Criando o record do producer
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        // Enviando dados de forma síncrona
        producer.send(producerRecord);
        producer.flush();

        // Flush e close do recurso producer
        producer.close();
    }
}