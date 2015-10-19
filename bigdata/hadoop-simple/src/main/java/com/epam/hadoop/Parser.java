package com.epam.hadoop;


import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

/**
 * @author Andrei_Yakushin
 * @since 10/19/2015 4:55 PM
 */
public class Parser {
    private static final String FOLDER = "hdfs://sandbox.hortonworks.com:8020/user/root/";
    private static final int LIMIT = 500000;
    private static final int LIST_LIMIT = 50000;

    private static final String NOT_SORTED = "not_sorted.txt";
    private static final String SORTED = "sorted.txt";

    private static String[] FILES = {"bid.20130311.txt.bz2", "bid.20130312.txt.bz2", "bid.20130313.txt.bz2", "bid.20130314.txt.bz2", "bid.20130315.txt.bz2", "bid.20130316.txt.bz2", "bid.20130317.txt.bz2"};

    private static int intermediateCount = 0;
    private static final Map<Integer, List<String>> intermediateFiles = new HashMap<Integer, List<String>>();

    public static void main(String[] args) throws IOException {
        PrintWriter log = new PrintWriter(new File("out.log"));

        try {
            FileSystem fs = FileSystem.get(new Configuration());
//            original(fs);
//            intermediate(fs);
            sort(fs);
            mergeSortIntermediate(fs);
        } catch (Throwable e) {
            e.printStackTrace(log);
        } finally {
            log.close();
        }
    }

    private static void original(FileSystem fs) throws IOException {
        TObjectIntMap<String> result = new TObjectIntHashMap<String>();
        for (String s : FILES) {
            BufferedReader reader = null;
            try {
                reader = getZipReader(FOLDER + s, fs);
                String line = reader.readLine();
                while (line != null) {
                    String key = extractOriginal(line);
                    result.adjustOrPutValue(key, 1, 1);
                    if (result.size() >= LIMIT) {
                        result = flush(result, fs);
                    }
                    line = reader.readLine();
                }
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }
    }

    private static TObjectIntMap<String> flush(TObjectIntMap<String> result, FileSystem fs) throws IOException {
        intermediateCount++;
        Map<String, BufferedWriter> files = new THashMap<>();
        try {
            for(TObjectIntIterator<String> it = result.iterator(); it.hasNext(); ) {
                it.advance();
                String key = it.key();
                char c = key.charAt(0);
                String name = "mid/" + c + "_flush_" + intermediateCount + ".txt";
                BufferedWriter writer = files.get(name);
                if (writer == null) {
                    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(FOLDER + name))));
                    files.put(name, writer);
                    List<String> list = intermediateFiles.get((int) c);
                    if (list == null) {
                        list = new ArrayList<String>();
                        intermediateFiles.put((int) c, list);
                    }
                    list.add(name);
                }
                writer.write(key + "\t" + it.value() + "\n");
            }
        } finally {
            for (BufferedWriter writer : files.values()) {
                writer.close();
            }
        }
        return new TObjectIntHashMap<String>();
    }

    private static void intermediate(FileSystem fs) throws IOException {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(FOLDER + NOT_SORTED))));
            for (List<String> files : intermediateFiles.values()) {
                BufferedReader reader = null;
                TObjectIntMap<String> result = new TObjectIntHashMap<>();
                for (String file : files) {
                    try {
                        reader = getReader(FOLDER + file, fs);
                        String line = reader.readLine();
                        while (line != null) {
                            int b = line.indexOf('\t');
                            String key = new String(line.substring(0, b));
                            int count = Integer.parseInt(line.substring(b + 1));
                            result.adjustOrPutValue(key, count, count);
                            line = reader.readLine();
                        }
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                    }
                }
                for (TObjectIntIterator<String> it = result.iterator(); it.hasNext(); ) {
                    it.advance();
                    writer.write(it.key() + "\t" + it.value() + "\n");
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private static void sort(FileSystem fs) throws IOException {
        intermediateCount = 0;
        intermediateFiles.clear();
        BufferedReader reader = null;
        TIntObjectMap<List<String>> result = new TIntObjectHashMap<List<String>>();
        try {
            reader = getReader(FOLDER + NOT_SORTED, fs);
            String line = reader.readLine();
            while (line != null) {
                int b = line.indexOf('\t');
                String key = new String(line.substring(0, b));
                int count = Integer.parseInt(line.substring(b + 1));
                List<String> list = result.get(count);
                if (list == null) {
                    list = new ArrayList<String>();
                    result.put(count, list);
                }
                list.add(key);
                if (list.size() >= LIST_LIMIT) {
                    list = flush(list, count, fs);
                    result.put(count, list);
                }
                line = reader.readLine();
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    private static void mergeSortIntermediate(FileSystem fs) throws IOException {
        List<Integer> counts = new ArrayList<>(intermediateFiles.keySet());
        Collections.sort(counts, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return - o1.compareTo(o2);
            }
        });
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(FOLDER + SORTED))));
            for (Integer count : counts) {
                List<String> files = intermediateFiles.get(count);
                BufferedReader reader = null;
                for (String file : files) {
                    try {
                        reader = getReader(FOLDER + file, fs);
                        String line = reader.readLine();
                        while (line != null) {
                            writer.write(line + "\t" + count + "\n");
                            line = reader.readLine();
                        }
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                    }
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private static List<String> flush(List<String> result, int number, FileSystem fs) throws IOException {
        intermediateCount++;
        BufferedWriter writer = null;
        try {
            intermediateCount++;
            String name = "mid/" + number + "_sort_" + intermediateCount + ".txt";
            writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(FOLDER + name))));
            List<String> list = intermediateFiles.get(number);
            if (list == null) {
                list = new ArrayList<String>();
                intermediateFiles.put(number, list);
            }
            list.add(name);

            for (String s : result) {
                writer.write(s + "\n");
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        return new ArrayList<>();
    }

    //------------------------------------------------------------------------------------------------------------------

    private static BufferedReader getReader(String path, FileSystem fs) throws IOException {
        Path pt = new Path(path);
        return new BufferedReader(new InputStreamReader(fs.open(pt)));
    }

    private static BufferedReader getZipReader(String path, FileSystem fs) throws IOException {
        Path pt = new Path(path);
        return new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(fs.open(pt))));
    }

    private static String extractOriginal(String line) {
        int start = line.indexOf('\t', line.indexOf('\t') + 1) + 1;
        return new String(line.substring(start, line.indexOf('\t', start)));
    }
}
