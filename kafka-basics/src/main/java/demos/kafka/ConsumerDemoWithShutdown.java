package demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        // creating consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainthread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                   consumer.wakeup();

                   // join the main thread to allow the execution of the code in the main thread
                   try {
                       mainthread.join();
                   } catch (InterruptedException e) {
                       e.printStackTrace();
                   }
               }));

        try {
            // subscribe consumer to our topics
            consumer.subscribe(Collections.singleton(topic));

            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("""
                                     Key: %s
                                     Value: %s
                                     Partition: %s
                                     Offset: %s
                                     """.formatted(record.key(),
                                                   record.value(),
                                                   record.partition(),
                                                   record.offset()));
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ingore this as this an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            // this will also commit the offsets if need be
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }
    }
}
