package gladiator.binlog;


import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.io.IOException;

/**
 * Run as mysql slave and publish all insert/update/delete events to kafka topics;
 * Each topic store events for corresponding table.
 * 2016/07/06
 */
public class BinaryLogClientBean {

    final BinaryLogClient client;
    protected final Logger logger = LoggerFactory.getLogger(BinaryLogClientBean.class);
    boolean debug;

    //EE: update batch records and test binlog parser performance. begin event as ..., end event as ...
    StopWatch stopWatch = new StopWatch();

    public BinaryLogClientBean(String hostname, int port, String username, String password, Boolean debug) {
        client = new BinaryLogClient(hostname, port, username, password);
        this.debug = debug;
    }

    /**
     * GTID event (if gtid_mode=ON) -> QUERY event with "BEGIN" as sql -> ... -> XID event | QUERY event with "COMMIT" or "ROLLBACK" as sql.
     *
     * @param event
     */
    private void txEventStats(Event event) {
        //Begin stopwatch once 'BEGIN' Query event;Stop stopwatch once 'XID' or 'COMMIT' event;
        if (event.getData() instanceof QueryEventData && ((QueryEventData) event.getData()).getSql().equals("BEGIN")) {
            stopWatch = new StopWatch();
            stopWatch.start("shyiko-tx-event-stat-" + ((QueryEventData) event.getData()).getDatabase());
            logger.info("SHYIKO-TX-Binlog-Parser StopWatch start...");
        } else if (event.getData() instanceof XidEventData) {
            stopWatch.stop();
            logger.info(stopWatch.toString());
        }
    }

    public void init() throws IOException {
        //EE: prevent blocking main thread during boot initialization.
        Thread t = new Thread() {
            @Override
            public void run() {
                //EE: binlog event listener
                logger.info("Initializing binaryLog client....");
                client.registerEventListener(new BinaryLogClient.EventListener() {

                    @Override
                    public void onEvent(Event event) {
                        if (debug)
                            logger.info(event.toString());

                        txEventStats(event);

//                        if (event.getData() instanceof WriteRowsEventData || event.getData() instanceof UpdateRowsEventData || event.getData() instanceof DeleteRowsEventData) {
//                            logger.info(event.toString());
//                            //TODO publish to kafka
//                        }
                    }
                });
                try {
                    client.connect();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        t.setName("binaryLog-client-thread");
        t.start();
    }

    public void cleanup() throws IOException {
        client.disconnect();
    }

    public BinaryLogClient getClient() {
        return client;
    }
}
