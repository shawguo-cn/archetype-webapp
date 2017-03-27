package gladiator.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaConsumerService {
    protected final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final Object lifecycleMonitor = new Object();
    AtomicBoolean running = new AtomicBoolean(true);
    Properties defaultConsumerProps;

    AtomicLong current = new AtomicLong(0);

    public KafkaConsumerService(Properties defaultConsumerProps) {
        this.defaultConsumerProps = defaultConsumerProps;
    }

    public String consume(String topic, long count) {
        //initialize consume context
        defaultConsumerProps.put("group.id", "kafka-consumer-server-" + new Date().getTime());
        current.set(0);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(defaultConsumerProps);
        consumer.subscribe(Arrays.asList(topic));
        StopWatch stopWatch = new StopWatch(String.format("consume topic - %s %d", topic, count));
        stopWatch.start();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            current.addAndGet(records.count());
            logger.info("polling records size: {}", records.count());
            if (records.isEmpty())
                continue;
            else {
                for (ConsumerRecord<String, String> record : records) {
                    logger.trace(record.toString());
                }
                consumer.commitSync();
            }
            if (current.get() >= count || !running.get())
                break;
        }
        logger.info("expected consume: {} actual consume: {}", count, current.get());
        stopWatch.stop();
        return running.get() ? stopWatch.toString() : "consumer stopped " + stopWatch.toString();
    }

    public String stop() {
        this.running.set(false);
        return "stop finished";
    }

    public String status() {
        return "Not Support";
    }

    public String reload(String path) throws IOException {
        synchronized (lifecycleMonitor) {
            this.running.set(false);
            Properties newProps = new Properties();
            newProps.load(new FileInputStream(path));
            newProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, defaultConsumerProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            this.defaultConsumerProps = newProps;
            return String.format("kafka consumer config is reload from %s, %s", path);
        }
    }

}
