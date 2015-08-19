package com.epam.mapreduce;

import com.epam.common.LengthPredicate;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class MapperProcessor {
    private final LengthPredicate predicate;

    @Inject
    public MapperProcessor(LengthPredicate predicate) {
        this.predicate = predicate;
    }

    public Iterator<String> process(String s) {
        return new WordIterator(predicate, s);
    }

    private static class WordIterator implements Iterator<String> {
        private static final Pattern PATTERN = Pattern.compile("[A-Za-z]([A-Za-z-]?[A-Za-z]+)*");

        private final LengthPredicate predicate;

        private final Matcher matcher;
        private int s;
        private boolean needToMove;

        public WordIterator(LengthPredicate predicate, String string) {
            this.predicate = predicate;
            matcher = PATTERN.matcher(string);
            s = 0;
            needToMove = true;
        }

        private void move() {
            if (needToMove) {
                while (matcher.find(s)) {
                    s = matcher.end();
                    String group = matcher.group();
                    if (predicate.test(group)) {
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
}
