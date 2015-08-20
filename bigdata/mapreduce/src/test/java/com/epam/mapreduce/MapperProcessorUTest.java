package com.epam.mapreduce;

import com.epam.common.LengthPredicate;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

@Test
public class MapperProcessorUTest {
    public static final String INPUT = "Towards the end of November, during a thaw, at nine o'clock one morning";
    public static final String[] OUTPUT = new String[] {
            "towards",
            "november",
            "during",
            "thaw",
            "nine",
            "clock",
            "morning"
    };

    public void test() {
        MapperProcessor processor = new MapperProcessor(new LengthPredicate(4));

        Iterator<String> process = processor.process(INPUT);
        for (String s : OUTPUT) {
            assertTrue(process.hasNext());
            String next = process.next();
            assertEquals(next, s);
        }
        assertFalse(process.hasNext());
    }
}
