package com.epam.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;

public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WordCount(), args));
    }

    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            return 2;
        }

        Job job = new Job(getConf(), "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCount.TheMapper.class);
        job.setReducerClass(WordCount.TheReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);

        @Inject
        private MapperProcessor processor;

        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            SpringContainer.requestInjections(this);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            for (Iterator<String> it = processor.process(value.toString()); it.hasNext();) {
                word.set(it.next());
                context.write(this.word, ONE);
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TheReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for (Iterator it = values.iterator(); it.hasNext(); sum += val.get()) {
                val = (IntWritable) it.next();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }
}