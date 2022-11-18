package org.apache.spark.sql.hbase.execution;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 每次插入rowkey + 1, 分布式环境下不可用
 */
public class DefaultRowKeyGenerator implements RowKeyGenerator {

    private static final AtomicInteger atomicInteger = new AtomicInteger(0);

    public byte[] genRowKey(Row row) {
        String rowKey = String.format("%04d", atomicInteger.addAndGet(1));
        return Bytes.toBytes(rowKey);
    }
}

