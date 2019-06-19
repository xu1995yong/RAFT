package com.xu.raft.handler;

import com.alibaba.fastjson.JSON;
import com.xu.raft.entity.LogEntry;
import com.xu.raft.node.NodeInfo;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Component
public class LogEntryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogEntryHandler.class);
    @Autowired
    private NodeInfo nodeInfo;

    @Value("${raft.db.dir}")
    private String dbDir;

    private RocksDB logDb;

    public static final byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();

    private ReentrantLock lock = new ReentrantLock();

    @PostConstruct
    private void init() {
        final String logsDir = dbDir + "/" + nodeInfo.getSelfPort() + "/LogModule";
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("logsDir is :{}", logsDir);
        }

        Options options = new Options();
        options.setCreateIfMissing(true);

        File file = new File(logsDir);
        boolean success = false;
        if (!file.exists()) {
            success = file.mkdirs();
        }
        if (success) {
            LOGGER.warn("make a new dir : " + logsDir);
        }
        try {
            logDb = RocksDB.open(options, logsDir);
        } catch (RocksDBException e) {
            LOGGER.warn(e.getMessage());
        }
    }


    /**
     * logEntry 的 index 就是 key. 严格保证递增.
     */
    public void write(LogEntry logEntry) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("DefaultLogModule write rocksDB success, logEntry info : [{}]", logEntry);
        }


        boolean success = false;
        try {
            lock.tryLock(3000, MILLISECONDS);
            logEntry.setIndex(getLastIndex() + 1);
            logDb.put(String.valueOf(logEntry.getIndex()).getBytes(), JSON.toJSONBytes(logEntry));
            success = true;
        } catch (RocksDBException | InterruptedException e) {
            LOGGER.warn(e.getMessage());
        } finally {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("DefaultLogModule write rocksDB success, logEntry info : [{}]", logEntry);
            }
            if (success) {
                updateLastIndex(logEntry.getIndex());
            }
            lock.unlock();
        }
    }


    public LogEntry read(long index) {
        try {
            byte[] result = logDb.get(String.valueOf(index).getBytes());
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            LOGGER.warn(e.getMessage(), e);
        }
        return null;
    }

    public void removeOnStartIndex(long startIndex) {
        boolean success = false;
        int count = 0;
        try {
            lock.tryLock(3000, MILLISECONDS);
            for (long i = startIndex; i <= getLastIndex(); i++) {
                logDb.delete(String.valueOf(i).getBytes());
                ++count;
            }
            success = true;
            LOGGER.warn("rocksDB removeOnStartIndex success, count={} startIndex={}, lastIndex={}", count, startIndex, getLastIndex());
        } catch (InterruptedException | RocksDBException e) {
            LOGGER.warn(e.getMessage());
        } finally {
            if (success) {
                updateLastIndex(getLastIndex() - count);
            }
            lock.unlock();
        }
    }


    public LogEntry getLast() {
        try {
            byte[] result = logDb.get(String.valueOf(getLastIndex()).getBytes());
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public long getLastIndex() {
        byte[] lastIndex = "-1".getBytes();
        try {
            lastIndex = logDb.get(LAST_INDEX_KEY);
            if (lastIndex == null) {
                lastIndex = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Long.valueOf(new String(lastIndex));
    }

    // on lock
    private void updateLastIndex(long index) {
        try {
            // overWrite
            logDb.put(LAST_INDEX_KEY, String.valueOf(index).getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


}
