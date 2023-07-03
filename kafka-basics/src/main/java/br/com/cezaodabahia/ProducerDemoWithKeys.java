package br.com.cezaodabahia;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Kafka producer");

        // Criando as propriedades do producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // Criando o producer - os generics correspondem aos tipos dos serializers
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord;
        for (int a = 0; a < 3; a++){
            for (int i = 0; i < 10; i++){

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World with Key " + i;

                // Criando o record do producer
                producerRecord = new ProducerRecord<>(topic, key, value);

                // Enviando dados
                producer.send(producerRecord, (metadata, exception) -> {
                    if (Objects.isNull(exception)){
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
                        //mesma key, mesma partition
                        log.info("Key: " + key + " | Partition: " + metadata.partition());
                    } else {
                        log.error("Error while producing", exception);
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.flush();

        // Flush e close do recurso producer
        producer.close();
    }
}