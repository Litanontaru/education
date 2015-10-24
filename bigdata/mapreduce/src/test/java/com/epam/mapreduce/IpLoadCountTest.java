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

/**
 * @author Andrei_Yakushin
 * @since 10/24/2015 7:36 PM
 */
@Test
public class IpLoadCountTest {
    public static final String IN1 = "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
    public static final String IN2 = "ip28 - - [24/Apr/2011:05:41:56 -0400] \"GET /sun3/ HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"";
    public static final String IN3 = "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss20/floppy.jpg HTTP/1.1\" 200 12433 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";

    MapDriver<Object, Text, Text, IpLoadCount.EntryWritable> mapDriver;
    ReduceDriver<Text, IpLoadCount.EntryWritable, Text, IpLoadCount.EntryWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IpLoadCount.EntryWritable, Text, IpLoadCount.EntryWritable> mapReduceDriver;

    @BeforeClass
    public void setUp() {
        IpLoadCount.TheMapper mapper = new IpLoadCount.TheMapper();
        IpLoadCount.TheReducer reducer = new IpLoadCount.TheReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    public void testMapper() throws IOException {
        mapDriver.withInput(new IntWritable(1), new Text(IN1));
        mapDriver.withInput(new IntWritable(1), new Text(IN2));
        mapDriver.withOutput(new Text("ip2"), new IpLoadCount.EntryWritable(14917, 1));
        mapDriver.runTest();
    }

    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("ip2"), Arrays.asList(new IpLoadCount.EntryWritable(14917, 1), new IpLoadCount.EntryWritable(12433, 1)));
        reduceDriver.withOutput(new Text("ip2"), new IpLoadCount.EntryWritable(27350, 2));
        reduceDriver.runTest();
    }

    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new IntWritable(1), new Text(IN1));
        mapReduceDriver.withInput(new IntWritable(2), new Text(IN2));
        mapReduceDriver.withInput(new IntWritable(3), new Text(IN3));
        mapReduceDriver.withOutput(new Text("ip2"), new IpLoadCount.EntryWritable(27350, 2));
        mapReduceDriver.runTest();
    }
}
