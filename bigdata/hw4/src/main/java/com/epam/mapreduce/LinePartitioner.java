package com.epam.mapreduce;

import org.apache.hadoop.io.ByteWritable;

/**
 * @author Andrei_Yakushin
 * @since 10/26/2015 8:47 PM
 */
public class LinePartitioner extends org.apache.hadoop.mapreduce.Partitioner<ByteWritable, NumberWritable> {
    @Override
    public int getPartition(ByteWritable key, NumberWritable value, int numPartitions) {
        if (key.get() == (byte) LineNumbering.C_MARKER) {
            return value.getPartition();
        } else {
            return LinePartitioner.partitionForValue(value, numPartitions);
        }
    }

    public static int partitionForValue(NumberWritable value, int numPartitions) {
        return (value.getValue().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
