package gladiator.rocksdb;

import dominus.web.GlobalConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("rocksdb")
@Configuration
public class RocksDBConfig extends GlobalConfig {

    @Bean(name = "rocksdb")
    public RocksDBPerfTestService rocksDBPerfTest() {
        RocksDBPerfTestService rocks = new RocksDBPerfTestService(env.getProperty("rocks.source.sql"),
                env.getProperty("rocks.source.count-sql"), env.getProperty("rocks.source.key"));

        return rocks;
    }
}
