package com.epam.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * @author Andrei_Yakushin
 * @since 10/24/2015 4:04 PM
 */
public class IpLoadCount extends Configured implements Tool {

    private static final String BROWSER = "BROWSER";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        // Compress MapReduce output
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        System.exit(ToolRunner.run(conf, new IpLoadCount(), args));
    }

    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.err.println("Usage: IpLoadCount <in> <out>");
            return 2;
        }

        Job job = new Job(getConf(), "ip count");
        job.setJarByClass(IpLoadCount.class);

        job.setMapperClass(IpLoadCount.TheMapper.class);
        job.setCombinerClass(IpLoadCount.TheReducer.class);
        job.setReducerClass(IpLoadCount.TheReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(EntryWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        int result = job.waitForCompletion(true) ? 0 : 1;
        /*Counters counters = job.getCounters();
        for (CounterGroup group : counters) {
            if (group.getName().equals(BROWSER)) {
                for (Counter counter : group) {
                    System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
                }
                break;
            }
        }*/

        return result;
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheMapper extends Mapper<Object, Text, Text, EntryWritable> {
        private static final Pattern PATTERN = Pattern.compile("\"[\\S ]+?\"|\\[[\\S ]+?\\]|\\S+");

        private Text ip = new Text();
        private EntryWritable count = new EntryWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            new ClassPathXmlApplicationContext("META-INF/spring/application-context.xml").getAutowireCapableBeanFactory().autowireBean(this);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            WordIterator it = new WordIterator(null, value.toString(), PATTERN);
            ip.set(it.next());
            it.next();
            it.next();
            it.next();
            it.next();
            it.next();
            String bytes = it.next();
            if (!Objects.equals(bytes, "-")) {
                try {
                    count.set(Integer.parseInt(bytes), 1);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("cant parse: " + value.toString());
                }
                /*context.write(ip, count);
                it.next();
                String browser = it.next();
                WordIterator browserExtractor = new WordIterator(null, browser);
                if (browserExtractor.hasNext()) {
                    context.getCounter(BROWSER, browserExtractor.next()).increment(1);
                }*/
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheReducer extends Reducer<Text, EntryWritable, Text, EntryWritable> {
        private EntryWritable result = new EntryWritable();

        @Override
        public void reduce(Text key, Iterable<EntryWritable> values, Context context) throws IOException, InterruptedException {
            this.result.set(0, 0);
            EntryWritable val;
            for (Iterator it = values.iterator(); it.hasNext(); this.result.add(val)) {
                val = (EntryWritable) it.next();
            }
            context.write(key, this.result);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class EntryWritable implements Writable {
        private IntWritable total;
        private IntWritable count;

        public EntryWritable() {
            total = new IntWritable();
            count = new IntWritable();
        }

        public EntryWritable(int total, int avg) {
            this.total = new IntWritable(total);
            this.count = new IntWritable(avg);
        }

        public IntWritable getTotal() {
            return total;
        }

        public IntWritable getCount() {
            return count;
        }

        public void set(int total, int count) {
            this.total.set(total);
            this.count.set(count);
        }

        public void add(EntryWritable that) {
            this.total.set(this.total.get() + that.total.get());
            this.count.set(this.count.get() + that.count.get());
        }

        public void write(DataOutput dataOutput) throws IOException {
            total.write(dataOutput);
            count.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            total.readFields(dataInput);
            count.readFields(dataInput);
        }

        @Override
        public String toString() {
            return total.toString() + ',' + (((double) total.get()) / count.get());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EntryWritable that = (EntryWritable) o;

            if (!total.equals(that.total)) return false;
            return count.equals(that.count);

        }

        @Override
        public int hashCode() {
            int result = total.hashCode();
            result = 31 * result + count.hashCode();
            return result;
        }
    }
}
