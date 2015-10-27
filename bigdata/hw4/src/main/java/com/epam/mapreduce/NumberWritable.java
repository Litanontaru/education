package com.epam.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author Andrei_Yakushin
 * @since 10/26/2015 8:41 PM
 */
public class NumberWritable implements Writable {
    private Text value;
    private long count;
    private int partition;

    private static final Text EMPTY_STRING = new Text("");

    public void setValue(Text value) {
        this.value = value;
        if (value.getLength() == 0) {
            this.count = 0;
            this.partition = 0;
        }
    }

    public void setCounter(int partition, long count) {
        this.value = EMPTY_STRING;
        this.partition = partition;
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public int getPartition() {
        return partition;
    }

    public Text getValue() {
        return value;
    }

    public void write(DataOutput out) throws IOException {
        value.write(out);
        if (value.getLength() == 0) {
            out.writeInt(partition);
            out.writeLong(count);
        }
    }

    public void readFields(DataInput in) throws IOException {
        if (value == null) {
            value = new Text();
        }
        value.readFields(in);
        if (value.getLength() == 0) {
            partition = in.readInt();
            count = in.readLong();
        } else {
            partition = 0;
            count = 0;
        }
    }

    @Override
    public String toString() {
        return "{" + value + '\t' + count + '\t' + partition + '}';
    }
}
