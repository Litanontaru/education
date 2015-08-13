package com.epam.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Andrei_Yakushin on 8/12/2015.
 */
public class EntryWritable implements WritableComparable<EntryWritable> {
    private Text key;
    private IntWritable value;

    public EntryWritable() {
        key = new Text();
        value = new IntWritable();
    }

    public EntryWritable(Text key, IntWritable value) {
        this.key = key;
        this.value = value;
    }

    public Text getKey() {
        return key;
    }

    public IntWritable getValue() {
        return value;
    }

    public void set(String key, int value) {
        this.key.set(key);
        this.value.set(value);
    }

    public int compareTo(EntryWritable that) {
        int compared = value.compareTo(that.value);
        return compared == 0 ? key.compareTo(that.getKey()) : -compared;
    }

    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        value.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        value.readFields(dataInput);
    }

    @Override
    public String toString() {
        return key.toString() + '\t' + value.toString();
    }
}
