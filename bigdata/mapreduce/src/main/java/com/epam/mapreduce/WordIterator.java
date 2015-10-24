package com.epam.mapreduce;

import com.epam.common.LengthPredicate;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andrei_Yakushin
 * @since 10/24/2015 4:50 PM
 */
public class WordIterator implements Iterator<String> {
    private static final Pattern DEFAULT_PATTERN = Pattern.compile("[A-Za-z]([A-Za-z-]?[A-Za-z]+)*");

    private final LengthPredicate predicate;

    private final Matcher matcher;
    private int s;
    private boolean needToMove;

    public WordIterator(LengthPredicate predicate, String string) {
        this(predicate, string, DEFAULT_PATTERN);
    }

    public WordIterator(LengthPredicate predicate, String string, Pattern pattern) {
        this.predicate = predicate;
        matcher = pattern.matcher(string);
        s = 0;
        needToMove = true;
    }

    private void move() {
        if (needToMove) {
            while (matcher.find(s)) {
                s = matcher.end();
                String group = matcher.group();
                if (predicate == null || predicate.test(group)) {
                    needToMove = false;
                    break;
                }
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
