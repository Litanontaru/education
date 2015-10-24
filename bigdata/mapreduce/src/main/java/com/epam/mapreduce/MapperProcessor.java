package com.epam.mapreduce;

import com.epam.common.LengthPredicate;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Iterator;

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
}
