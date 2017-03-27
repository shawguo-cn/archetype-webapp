package gladiator.rocksdb;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StopWatch;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load big table into RocksDB.
 * Test Insert/Query performance.
 */
public class RocksDBPerfTestService {

    static final String ROCKS_PATH = "/tmp/rocksdb";
    @Autowired
    public JdbcTemplate template;

    protected final Logger logger = LoggerFactory.getLogger(RocksDBPerfTestService.class);
    RocksDB rocksDB = null;
    Options options;
    String sourceSql;
    String sourceCountSql;
    String sourceKey;
    ObjectMapper mapper;

    public RocksDBPerfTestService(String sourceSql, String sourceCountSql, String sourceKey) {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        options = new Options().setCreateIfMissing(true);
        rocksDB = null;
        try {
            // a factory method that returns a RocksDB instance
            rocksDB = RocksDB.open(options, ROCKS_PATH);
            // do something
        } catch (RocksDBException e) {
            // do some error handling
            logger.error(e.toString());
        }

        mapper = new ObjectMapper();// create once, reuse
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.setDateFormat(new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"));

        this.sourceCountSql = sourceCountSql;
        this.sourceSql = sourceSql;
        this.sourceKey = sourceKey;
    }

    public String load() throws SQLException, IOException, RocksDBException {
        logger.info("rocksdb.load");
        long count = template.queryForObject(sourceCountSql, Long.class);
        logger.info("total count:{}", count);
        AtomicLong now = new AtomicLong(0);
        BasicRowProcessor rowProcessor = new BasicRowProcessor();

        //EE:mysql streaming result set
        StopWatch watch = new StopWatch("rocksdb load " + count);
        watch.start();
        Statement statement = template.getDataSource().getConnection().
                createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = statement.executeQuery(sourceSql);
        while (rs.next()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            mapper.writeValue(out, rowProcessor.toMap(rs));
            rocksDB.put(Long.valueOf(rs.getLong(sourceKey)).toString().getBytes(), out.toByteArray());
            logger.info("{}/{}, now {}%...", now.incrementAndGet(), count, now.get() * 100 / count);
        }
        watch.stop();
        logger.info("{} size:{}", ROCKS_PATH, FileUtils.sizeOfDirectory(new File(ROCKS_PATH)));
        return watch.toString();
    }

    public void reload() throws RocksDBException, SQLException, IOException {
        logger.info("rocksdb.reload");
        this.clean();
        this.load();
    }

    public String clean() throws IOException, RocksDBException {

        rocksDB.close();

        logger.info("rocksdb.clean");
        logger.info("{} size:{}", ROCKS_PATH, FileUtils.sizeOf(new File(ROCKS_PATH)));
        FileUtils.deleteDirectory(new File(ROCKS_PATH));

        rocksDB = RocksDB.open(options, ROCKS_PATH);
        return ROCKS_PATH + " is deleted";
    }

    public String query(String key) throws RocksDBException {
        logger.info("rocksdb.query");
        return new String(rocksDB.get(key.getBytes()));
    }

}
