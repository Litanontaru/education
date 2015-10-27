package com.epam.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Andrei_Yakushin
 * @since 10/26/2015 8:35 PM
 */
public class LineNumbering extends Configured implements Tool {
    public final static byte C_MARKER = (byte) 'T';
    public final static byte V_MARKER = (byte) 'M';

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new LineNumbering(), args));
    }

    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.err.println("Usage: LineNumbering <in> <out>");
            return 2;
        }

        Job job = new Job(getConf(), "ip count");
        job.setJarByClass(LineNumbering.class);

        job.setGroupingComparatorClass(NopComparator.class);
        job.setPartitionerClass(LinePartitioner.class);

        job.setMapperClass(LineNumbering.TheMapper.class);
        job.setMapOutputKeyClass(ByteWritable.class);
        job.setMapOutputValueClass(NumberWritable.class);

        job.setReducerClass(LineNumbering.TheReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheMapper extends Mapper<Object, Text, ByteWritable, NumberWritable> {
        private long[] numbers;
        private int reduceTasks;

        private NumberWritable outputValue = new NumberWritable();
        private ByteWritable outputKey = new ByteWritable();

        protected void setup(Context context) throws IOException, InterruptedException {
            reduceTasks = context.getNumReduceTasks();
            numbers = new long[reduceTasks];
             outputKey.set(V_MARKER);
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            outputValue.setValue(value);
            context.write(outputKey, outputValue);
            numbers[LinePartitioner.partitionForValue(outputValue, reduceTasks)]++;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputKey.set(C_MARKER);
            for(int c = 0; c < numbers.length - 1; c++) {
                if (numbers[c] > 0) {
                    outputValue.setCounter(c + 1, numbers[c]);
                    context.write(outputKey, outputValue);
                }
                numbers[c + 1] += numbers[c];
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheReducer extends Reducer<ByteWritable, NumberWritable, Text, Text> {
        private Text outputKey = new Text();

        protected void setup(Context context) throws IOException, InterruptedException {
        }

        protected void reduce(ByteWritable key, Iterable<NumberWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<NumberWritable> itr = values.iterator();
            if (!itr.hasNext()) {
                return;
            }

            long offset = 0;
            NumberWritable value = itr.next();
            while (itr.hasNext() && value.getCount() > 0) {
                offset += value.getCount();
                value = itr.next();
            }
            outputKey.set(Long.toString(offset++));
            if (value.getPartition() == 0) {
                context.write(outputKey, value.getValue());
            }

            while(itr.hasNext()) {
                value = itr.next();
                outputKey.set(Long.toString(offset++));
                if (value.getPartition() == 0) {
                    context.write(outputKey, value.getValue());
                }
            }
        }
    }
}