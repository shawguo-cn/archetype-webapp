package gladiator.kafka;


import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class KafkaProducerService {

    protected final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private final Object lifecycleMonitor = new Object();

    //config
    Properties defaultProducerProps;
    String topic;
    boolean sync = true;

    //producer status
    Long numRecords;
    AtomicLong success = new AtomicLong(0);
    AtomicLong error = new AtomicLong(0);
    AtomicBoolean running = new AtomicBoolean(true);

    //kafka performance test
    int throughput = -1;
    //kafka.perf.default.recordsize=1024
    //kafka.perf.default.throughput=10000

    ConcurrentMap<String, AtomicInteger> statMap = new ConcurrentHashMap<>();

    public KafkaProducerService(Properties defaultProducerProps) {
        this.defaultProducerProps = defaultProducerProps;
    }

    public String produce(String topic, int recordSize, long numRecords) throws IOException, InterruptedException, ExecutionException, TimeoutException {

        synchronized (lifecycleMonitor) {

            resetCounter();
            this.numRecords = numRecords;
            KafkaProducer producer = new KafkaProducer(defaultProducerProps);

            /* setup perf test */
            byte[] payload = new byte[recordSize];
            Random random = new Random(0);
            for (int i = 0; i < payload.length; ++i)
                payload[i] = (byte) (random.nextInt(26) + 65);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, payload);
            Stats stats = new Stats(numRecords, 5000);
            long startMs = System.currentTimeMillis();
            StopWatch watch = new StopWatch("kafka producer send " + numRecords);
            watch.start();

            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);
            for (int i = 0; i < numRecords; i++) {
                long sendStartMs = System.currentTimeMillis();
                Callback cb = stats.nextCompletion(sendStartMs, payload.length, stats);
                try {
                    if (!sync) producer.send(record, cb);
                    else producer.send(record, cb).get(5000, TimeUnit.SECONDS); //async send

                    success.getAndIncrement();
                    if (throttler.shouldThrottle(i, sendStartMs)) {
                        throttler.throttle();
                    }
                } catch (Exception exception) {
                    logger.error(exception.toString());
                    error.incrementAndGet();
                    statMap.putIfAbsent(exception.getClass().getName(), new AtomicInteger(0));
                    statMap.get(exception.getClass().getName()).incrementAndGet();
                }

                if (!running.get()) {
                    String msg = String.format("kafka producer stopped. %d/%d, error:%d\n", success.longValue(), this.numRecords, error.longValue());
                    producer.close();
                    return msg;
                }
            }
            producer.close();
            stats.printTotal();


            watch.stop();
            producer.close();
            return watch.toString();
        }
    }

    public String stop() {
        this.running.set(false);
        return "stop finished";
    }

    public String status() {
        return String.format("%s %d/%d, error:%d %s", sync ? "sync producer" : "async producer",
                success.longValue(), numRecords, error.longValue(), printStatMap());//TODO message ratio
    }

    public String sync(boolean sync) {
        this.sync = sync;
        return String.format("use %s producer.", this.sync ? "sync" : "async");
    }

    public String reload(String path) throws IOException {
        synchronized (lifecycleMonitor) {
            this.running.set(false);
            Properties newProps = new Properties();
            newProps.load(new FileInputStream(path));
            newProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, defaultProducerProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            this.defaultProducerProps = newProps;
            return String.format("kafka config is reload from %s, %s", path, sync ? "sync producer" : "async producer");
        }
    }

    private void resetCounter() {
        success.set(0);
        numRecords = 0L;
        error.set(0);
        running.set(true);
        statMap.clear();
    }

    private String printStatMap() {
        StringBuffer sb = new StringBuffer();
        for (String exception : statMap.keySet()) {
            sb.append(exception + ":" + statMap.get(exception) + ",");
        }
        return sb.toString();
    }


    //EE: migrate from org.apache.kafka.tools.ProducerPerformance
    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }
}
