package com.epam.mapreduce;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.RawComparator;

/**
 * @author Andrei_Yakushin
 * @since 10/26/2015 8:45 PM
 */
public class NopComparator implements RawComparator<ByteWritable> {
    public int compare(ByteWritable left, ByteWritable right) {
        return 0;
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return 0;
    }
}
