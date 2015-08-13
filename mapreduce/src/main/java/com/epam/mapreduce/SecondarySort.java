package com.epam.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by Andrei_Yakushin on 8/12/2015.
 */
public class SecondarySort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: SecondarySort <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "secondary sort");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(SecondarySort.TheMapper.class);
        job.setReducerClass(SecondarySort.TheReducer.class);
        job.setOutputKeyClass(EntryWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(ThePartitioner.class);
        job.setGroupingComparatorClass(TheGroupingComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheMapper extends Mapper<Object, Text, EntryWritable, NullWritable> {
        private static final NullWritable NULL = NullWritable.get();
        private final EntryWritable entry = new EntryWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (split.length == 2) {
                try {
                    entry.set(split[0], Integer.parseInt(split[1]));
                    context.write(entry, NULL);
                } catch (NumberFormatException e) {
                    //do nothing
                }
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheReducer extends Reducer<EntryWritable, NullWritable, EntryWritable, NullWritable> {
        @Override
        protected void reduce(EntryWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static class ThePartitioner extends Partitioner<EntryWritable, NullWritable> {
        @Override
        public int getPartition(EntryWritable entryWritable, NullWritable nullWritable, int i) {
            return entryWritable.getValue().toString().hashCode() % i;
        }
    }

    public static class TheGroupingComparator extends WritableComparator {
        public TheGroupingComparator() {
            super(EntryWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable thisValue = ((EntryWritable) a).getValue();
            IntWritable thatValue = ((EntryWritable) b).getValue();
            return thisValue.compareTo(thatValue);
        }
    }
}