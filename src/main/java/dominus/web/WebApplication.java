package dominus.web;


import gladiator.binlog.BinlogConfig;
import gladiator.kafka.KafkaConfig;
import gladiator.rocksdb.RocksDBConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({GlobalConfig.class, BinlogConfig.class, RocksDBConfig.class, KafkaConfig.class})
public class WebApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(WebApplication.class, args);
    }
}
