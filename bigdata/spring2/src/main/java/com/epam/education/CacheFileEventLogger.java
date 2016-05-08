package com.epam.education;

import java.util.List;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 10:38 AM
 */
public class CacheFileEventLogger extends FileEventLogger {
    private int cacheSize;
    private List<Event> cache;

    public CacheFileEventLogger(String filename) {
        super(filename);
    }

    @Override
    public void logEvent(Event event) {
        cache.add(event);
        if (cache.size() >= cacheSize) {
            flush();
            cache.clear();
        }
    }

    private void flush() {
        for (Event e : cache) {
            super.logEvent(e);
        }
    }

    public void destroy() {
        flush();
    }
}
