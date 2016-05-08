package com.epam.education;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 10:38 AM
 */
public class CacheFileEventLogger extends FileEventLogger {
    private int cacheSize;
    private List<Pair<Event, EventType>> cache = new ArrayList<Pair<Event, EventType>>();

    public CacheFileEventLogger(String filename) {
        super(filename);
    }

    @Override
    public void logEvent(Event event, EventType eventType) {
        cache.add(new Pair<Event, EventType>(event, eventType));
        if (cache.size() >= cacheSize) {
            flush();
            cache.clear();
        }
    }

    private void flush() {
        for (Pair<Event, EventType> pair : cache) {
            super.logEvent(pair.getKey(), pair.getValue());
        }
    }

    public void destroy() {
        flush();
    }
}
