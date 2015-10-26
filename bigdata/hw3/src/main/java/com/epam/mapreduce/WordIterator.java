package com.epam.mapreduce;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andrei_Yakushin
 * @since 10/24/2015 4:50 PM
 */
public class WordIterator implements Iterator<String> {
    private static final Pattern DEFAULT_PATTERN = Pattern.compile("[A-Za-z]([A-Za-z-]?[A-Za-z]+)*");

    private final Matcher matcher;
    private int s;
    private boolean needToMove;

    public WordIterator(String string) {
        this(string, DEFAULT_PATTERN);
    }

    public WordIterator(String string, Pattern pattern) {
        matcher = pattern.matcher(string);
        s = 0;
        needToMove = true;
    }

    private void move() {
        if (needToMove) {
            if (matcher.find(s)) {
                s = matcher.end();
                needToMove = false;
            }
        }
    }

    public boolean hasNext() {
        move();
        return !needToMove;
    }

    public String next() {
        move();
        needToMove = true;
        return matcher.group().toLowerCase();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
