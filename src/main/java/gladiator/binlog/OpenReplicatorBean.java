package gladiator.binlog;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class OpenReplicatorBean {
    protected final Logger logger = LoggerFactory.getLogger(OpenReplicatorBean.class);
    final OpenReplicator openReplicator;
    boolean debug;
    StopWatch stopWatch = new StopWatch();

    public OpenReplicatorBean(String hostname, int port, String username, String password, Boolean debug) {
        openReplicator = new OpenReplicator();
        openReplicator.setUser(username);
        openReplicator.setPassword(password);
        openReplicator.setHost(hostname);
        openReplicator.setPort(port);
        openReplicator.setServerId(6789);
        openReplicator.setLevel2BufferSize(50 * 1024 * 1024);
        openReplicator.setHeartbeatPeriod(0.5f);
        this.debug = debug;

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://" + hostname);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.prepareStatement("show master status;").executeQuery();
            rs.next();
            logger.info("show master status : {}, {}", rs.getString("File"), rs.getLong("Position"));
            openReplicator.setBinlogFileName(rs.getString("File"));
            openReplicator.setBinlogPosition(rs.getLong("Position"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void txEventStats(BinlogEventV4 event) {
        //Begin stopwatch once 'BEGIN' Query event;Stop stopwatch once 'XID' or 'COMMIT' event;
        if (event instanceof QueryEvent && ((QueryEvent) event).getSql().toString().contains("BEGIN")) {
            stopWatch = new StopWatch();
            stopWatch.start("open-replicator-tx-event-stat-" + ((QueryEvent) event).getDatabaseName());
            logger.info("Open-Replicator-TX-Binlog-Parser StopWatch start...");
        } else if (event instanceof XidEvent) {
            stopWatch.stop();
            logger.info(stopWatch.toString());
        }
    }

    public void init() throws IOException {


        Thread t = new Thread() {
            @Override
            public void run() {
                openReplicator.setBinlogEventListener(new BinlogEventListener() {
                    public void onEvents(BinlogEventV4 event) {
                        if (debug)
                            logger.info(event.toString());
                        txEventStats(event);
                    }
                });
                try {
                    openReplicator.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        };
        t.setName("open-replicator-binlog-parser-thread");
        t.start();
    }

    public void cleanup() throws IOException {
        openReplicator.stopQuietly(10, TimeUnit.SECONDS);
    }
}
