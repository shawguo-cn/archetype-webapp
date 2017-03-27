package gladiator.binlog;


import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.XidLogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

public class CanalLogFetcherBean {
    protected final Logger logger = LoggerFactory.getLogger(CanalLogFetcherBean.class);
    MysqlConnection mysqlConnection;
    StopWatch stopWatch = new StopWatch();
    boolean debug;

    public CanalLogFetcherBean(String hostname, int port, String username, String password, Boolean debug) {
        mysqlConnection = new MysqlConnection(new InetSocketAddress(hostname, port), username, password);
        this.debug = debug;
    }

    private void txEventStats(LogEvent event) {
        //Begin stopwatch once 'BEGIN' Query event;Stop stopwatch once 'XID' or 'COMMIT' event;
        if (event instanceof QueryLogEvent && ((QueryLogEvent) event).getQuery().contains("BEGIN")) {
            stopWatch = new StopWatch();
            stopWatch.start("canal-tx-event-stat-" + (((QueryLogEvent) event).getDbName()));
            logger.info("Canal-TX-Binlog-Parser StopWatch start...");
        } else if (event instanceof XidLogEvent) {
            stopWatch.stop();
            logger.info(stopWatch.toString());
        }
    }

    public void init() throws IOException {

        mysqlConnection.connect();
        Assert.isTrue(mysqlConnection.isConnected(), "mysql connector failed to connect to database");
        DirectLogFetcher fetcher = new DirectLogFetcher(mysqlConnection.getConnector().getReceiveBufferSize());
        fetcher.start(mysqlConnection.getConnector().getChannel());
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();

        //EE:update settings
        mysqlConnection.update("set wait_timeout=9999999");
        mysqlConnection.update("set net_write_timeout=1800");
        mysqlConnection.update("set net_read_timeout=1800");
        // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
        mysqlConnection.update("set names 'binary'");
        // mysql5.6针对checksum支持需要设置session变量
        // 如果不设置会出现错误： Slave can not handle replication events with the
        // checksum that master is configured to log
        // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
        mysqlConnection.update("set @master_binlog_checksum= '@@global.binlog_checksum'");

        //EE:query current binlog position and begin dump.
        ResultSetPacket packet = mysqlConnection.query("show master status");
        List<String> fields = packet.getFieldValues();
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = fields.get(0);
        binlogDumpCmd.binlogPosition = Long.valueOf(fields.get(1));
        binlogDumpCmd.slaveServerId = 3344;
        byte[] cmdBody = binlogDumpCmd.toBytes();
        logger.info("COM_BINLOG_DUMP with position:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(mysqlConnection.getConnector().getChannel(), new ByteBuffer[]{ByteBuffer.wrap(binlogDumpHeader.toBytes()),
                ByteBuffer.wrap(cmdBody)});
        logger.info("Begin to dump binlog...");


        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    LogEventConvert convert = new LogEventConvert();
                    while (true) {
                        if (fetcher.fetch()) {
                            LogEvent event = decoder.decode(fetcher, context);
                            if (debug)
                                logger.info(event.toString());
                            //convert to protocol buffer
                            CanalEntry.Entry entry = convert.parse(event);
                            txEventStats(event);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        };
        t.setName("canal-binlog-parser-thread");
        t.start();
    }

    public void cleanup() throws IOException {
        mysqlConnection.disconnect();
    }


}
