package demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java",
                                                                                 "hello world " + i);

            // send data - asynchronous
            producer.send(producerRecord, (metadata, e) -> {
                // executes every tome a record is successfully send or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    log.info(""" 
                                     Received new metadata
                                     Topic: %s
                                     Partition: %s
                                     Offset: %s
                                     Timestamp: %s
                                     """.formatted(
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            metadata.timestamp()
                    ));
                }
                else {
                    log.error("Error while producing", e);
                }
            });
        }

        // flush and close the Producer - synchronous
        producer.flush();
        producer.close();
    }
}
