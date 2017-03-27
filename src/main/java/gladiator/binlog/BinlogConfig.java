package gladiator.binlog;

import dominus.web.GlobalConfig;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.*;

import javax.sql.DataSource;

@Profile("binlog")
@Configuration
public class BinlogConfig extends GlobalConfig {

    @Bean(initMethod = "init", destroyMethod = "cleanup")
    public Object binaryLogParserBean() {

        String host = env.getProperty("mysql.hostname.cdc");
        int port = Integer.valueOf(env.getProperty("mysql.port.cdc"));
        String user = env.getProperty("mysql.username.cdc");
        String password = env.getProperty("mysql.password.cdc");
        Boolean debug = Boolean.valueOf(env.getProperty("binlog.parser.debug"));

        if (env.getProperty("binlog.parser").equalsIgnoreCase("shyiko")) {
            return new BinaryLogClientBean(host, port, user, password, debug);
        } else if (env.getProperty("binlog.parser").equalsIgnoreCase("dbsync")) {
            return new CanalLogFetcherBean(host, port, user, password, debug);
        } else if (env.getProperty("binlog.parser").equalsIgnoreCase("google")) {
            return new OpenReplicatorBean(host, port, user, password, debug);
        }
        return null;
    }

    @Bean
    @ConfigurationProperties(prefix = "binlog-generator.datasource")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "binlog-generator")
    public BinLogGeneratorService binLogGeneratorService() {
        return new BinLogGeneratorService(this.dataSource());
    }
}
