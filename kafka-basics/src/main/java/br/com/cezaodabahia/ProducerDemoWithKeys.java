package br.com.cezaodabahia;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());
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

        // Enviando dados
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (Objects.isNull(exception)){
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
                    log.info("Received new metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + formatter.format(new Timestamp(metadata.timestamp()).toLocalDateTime()));
                } else {
                    log.error("Error while producing", exception);
                }
            }
        });
        producer.flush();

        // Flush e close do recurso producer
        producer.close();
    }
}