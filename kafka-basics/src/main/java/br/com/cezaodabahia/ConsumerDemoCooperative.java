package br.com.cezaodabahia;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
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

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){

            // Referencia para a thread main
            final Thread mainThread = Thread.currentThread();

            // adicionando um hook de shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run() {
                    log.info("Shutdown detected, waking up...");
                    consumer.wakeup();
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            // Assinando um tópico
            consumer.subscribe(List.of(topic));

            // Lendo dados do tópico

            while (true){
                ConsumerRecords<String, String> records = consumer.poll(3000);

                for (ConsumerRecord<String, String> record: records){
                    log.info("Key: " + record.key() + " | Value: " + record.value());
                    log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            log.info("The consumer is shut down");
        }

    }
}