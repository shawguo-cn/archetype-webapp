package gladiator.binlog;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * select all PK into memory;
 * update records one by one in separate transaction to mock continuous small tx binlog stream;
 */
public class BinLogGeneratorService {

    protected final Logger logger = LoggerFactory.getLogger(BinLogGeneratorService.class);

    final static String LOAD_PK_SQL = "select %s from %s";
    final static String UPDATE_TIMESTAMP_SQL = "update %s set %s=? where %s=?";
    BlockingQueue<Long> pkBlockingQueue;
    Long total;
    final AtomicLong count = new AtomicLong(0);
    AtomicBoolean running = new AtomicBoolean(true);

    JdbcTemplate jdbcTemplate;

    public BinLogGeneratorService(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
        logger.info("{} is initialized", dataSource.getClass().getName());
    }

    public String loadPK(String table, String pkColumn) {
        List<Long> pkList = jdbcTemplate.queryForList(String.format(LOAD_PK_SQL, pkColumn, table), Long.class);
        pkBlockingQueue = new ArrayBlockingQueue<>(pkList.size(), false, pkList);
        total = Long.valueOf(pkList.size());

        String echo = String.format("load %s PKs from %s.%s", pkList.size(), table, pkColumn);
        logger.info(echo);
        return echo;
    }

    public String updateTimestamp(String table, String pkColumn, String timestampColumn, int concurrency) throws ExecutionException, InterruptedException {

        if (pkBlockingQueue == null || pkBlockingQueue.size() == 0)
            this.loadPK(table, pkColumn);

        StopWatch stopWatch = new StopWatch(String.format("%s.%s update timestamp in %d records", table, pkColumn, pkBlockingQueue.size()));
        stopWatch.start();

        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("binlog-generator-thread-");
        final AtomicLong stat = new AtomicLong(0);
        count.set(0);
        running.set(true);
        final Long startTime = System.currentTimeMillis();
        List<Future<Long>> result = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            result.add(executor.submit(() -> {
                long localCount = 0;
                while (!pkBlockingQueue.isEmpty() && running.get()) {
                    Long pk = pkBlockingQueue.poll();
                    if (pk != null) {
                        jdbcTemplate.update(String.format(UPDATE_TIMESTAMP_SQL,
                                table, timestampColumn, pkColumn), new Date(), pk);
                        //db profiler
                        if (stat.getAndIncrement() % 10000 == 0) {
                            long elapsed = System.currentTimeMillis() - startTime;
                            logger.info("consumed:{}/total:{}, tps: {}", count.get(), total, (stat.get() * 1000) / elapsed);
                        }
                        localCount++;
                        count.incrementAndGet();
                    }
                }
                logger.info("{} : {}", Thread.currentThread().getName(), localCount);
                return localCount;
            }));
        }
        long actual = 0;
        for (int i = 0; i < concurrency; i++)
            actual += result.get(i).get();
        Assert.isTrue(running.get() ? actual == total : true, String.format("expected:%s,actual:%s", total, actual));

        stopWatch.stop();
        return stopWatch.toString();
    }

    public String stop() {
        this.running.set(false);
        this.pkBlockingQueue.clear();
        return String.format("binlog generator is stopped. %s/%s", count, total);
    }
}
