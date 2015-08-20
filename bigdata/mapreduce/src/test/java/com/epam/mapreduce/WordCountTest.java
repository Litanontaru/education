package com.epam.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Test
public class WordCountTest {
    public static final String IN = "aaaa bbbb bbbb cccc cccc dddd eeee bbbb aaaa";
    private static final List<String> OUT = Arrays.asList("aaaa", "bbbb", "bbbb", "cccc", "cccc", "dddd", "eeee", "bbbb", "aaaa");

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @BeforeClass
    public void setUp() {
        WordCount.TheMapper mapper = new WordCount.TheMapper();
        WordCount.TheReducer reducer = new WordCount.TheReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    public void testMapper() throws IOException {
        mapDriver.withInput(new IntWritable(1), new Text(IN));
        for (String s : OUT) {
            mapDriver.withOutput(new Text(s), new IntWritable(1));
        }
        mapDriver.runTest();
    }

    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new IntWritable(1), new IntWritable(1)));
        reduceDriver.withOutput(new Text("1"), new IntWritable(2));
        reduceDriver.runTest();
    }

    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new IntWritable(1), new Text(IN));
        mapReduceDriver.withOutput(new Text("aaaa"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("bbbb"), new IntWritable(3));
        mapReduceDriver.withOutput(new Text("cccc"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("dddd"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("eeee"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}
