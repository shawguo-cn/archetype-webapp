package gladiator.kafka;


import dominus.web.GlobalConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Properties;

@Profile("kafka")
@Configuration
public class KafkaConfig extends GlobalConfig {

    @Bean(name = "kafka-producer")
    public KafkaProducerService kafkaProducerService() {
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put("bootstrap.servers", env.getProperty("kafka.bootstrap.servers"));
        kafkaProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducerProps.put("acks", "all");
        kafkaProducerProps.put("batch.size", 0);
        kafkaProducerProps.put("retries", 2);

        KafkaProducerService kafkaProducerService = new KafkaProducerService(kafkaProducerProps);
        return kafkaProducerService;
    }

    @Bean(name = "kafka-consumer")
    public KafkaConsumerService kafkaConsumerService() {
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", env.getProperty("kafka.bootstrap.servers"));
        kafkaConsumerProps.put("auto.offset.reset", "earliest");
        kafkaConsumerProps.put("enable.auto.commit", "false");
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("max.partition.fetch.bytes", "1048576"); //1Mb

        KafkaConsumerService kafkaConsumerService = new KafkaConsumerService(kafkaConsumerProps);
        return kafkaConsumerService;
    }
}
