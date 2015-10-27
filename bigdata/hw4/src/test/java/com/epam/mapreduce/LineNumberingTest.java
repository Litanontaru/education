package com.epam.mapreduce;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author Andrei_Yakushin
 * @since 10/27/2015 10:18 AM
 */

@Test
public class LineNumberingTest {
    public static final String[] IN = {
            "2e72d1bd7185fb76d69c852c57436d37",
            "93074d8125fa8945c5a971c2374e55a8",
            "bcbc973f1a93e22de83133f360759f04",
            "104128702bf24f58a8e215002ae851af",
            "d503d08833987e6c792dc45ea1810800",
            "710f5852a9bec40561ea85d7ff51a4e6",
            "b4791d1310887ab13162503d51696ad3",
            "18ad4741e0425bd23b93ad8ec3893279",
            "40d28159e587158a3a06ac5a3169727b",
            "f3e5ac43a95b759074a67c856b142055"
    };

    MapReduceDriver<Object, Text, ByteWritable, NumberWritable, Text, Text> mapReduceDriver;

    @BeforeClass
    public void setUp() {
        LineNumbering.TheMapper mapper = new LineNumbering.TheMapper();
        LineNumbering.TheReducer reducer = new LineNumbering.TheReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.getConfiguration().set("mapreduce.job.reduces", "5");
    }

    public void testMapReduce() throws IOException {
        for (int i = 0; i < IN.length; i++) {
            String s = IN[i];
            mapReduceDriver.withInput(new IntWritable(i), new Text(s));
        }
        for (int i = 0; i < IN.length; i++) {
            String s = IN[i];
            mapReduceDriver.withOutput(new Text(String.valueOf(i)), new Text(s));
        }
        mapReduceDriver.runTest();
    }
}
