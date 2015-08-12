package com.epam.mapreduce;

/**
 * Created by Andrei_Yakushin on 8/11/2015.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountOld extends Configured {
    public static class TheMapper implements Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private static final Pattern PATTERN = Pattern.compile("[A-Za-z]([A-Za-z-]*[A-Za-z])?");

        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            Matcher matcher = PATTERN.matcher(value.toString());
            int s = 0;
            while (matcher.find(s)) {
                String group = matcher.group();
                this.word.set(group.toLowerCase());
                output.collect(this.word, ONE);
                s = matcher.end();
            }
        }

        public void close() throws IOException {

        }

        public void configure(JobConf jobConf) {

        }
    }

    public static class TheReducer implements Reducer<Text, IntWritable, WeightedValueText, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<WeightedValueText, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            IntWritable val;
            for (; values.hasNext(); sum += val.get()) {
                val = values.next();
            }
            IntWritable result = new IntWritable(sum);
            output.collect(new WeightedValueText(key, result), result);
        }

        public void close() throws IOException {

        }

        public void configure(JobConf jobConf) {

        }
    }

    public static class WeightedValueText implements WritableComparable<WeightedValueText> {
        private Text key = new Text();
        private IntWritable value = new IntWritable();

        public WeightedValueText(Text key, IntWritable value) {
            this.key = key;
            this.value = value;
        }

        public void set(Text key, IntWritable value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(WeightedValueText that) {
            IntWritable thisValue = this.value;
            IntWritable thatValue = that.value;
            return thisValue.compareTo(thatValue);
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
            return key.toString();
        }
    }
}
