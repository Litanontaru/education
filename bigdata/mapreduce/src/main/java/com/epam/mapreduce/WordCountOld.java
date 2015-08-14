package com.epam.mapreduce;

/**
 * Created by Andrei_Yakushin on 8/11/2015.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

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

    public static class TheReducer implements Reducer<Text, IntWritable, EntryWritable, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<EntryWritable, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            IntWritable val;
            for (; values.hasNext(); sum += val.get()) {
                val = values.next();
            }
            IntWritable result = new IntWritable(sum);
            output.collect(new EntryWritable(key, result), result);
        }

        public void close() throws IOException {

        }

        public void configure(JobConf jobConf) {

        }
    }
}
