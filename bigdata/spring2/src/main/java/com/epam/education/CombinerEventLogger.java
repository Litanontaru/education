package com.epam.education;

import java.util.Collection;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 11:11 AM
 */
public class CombinerEventLogger implements EventLogger {
    private Collection<EventLogger> loggers;

    public CombinerEventLogger(Collection<EventLogger> loggers) {
        this.loggers = loggers;
    }

    public void logEvent(Event event, EventType eventType) {
        for (EventLogger logger : loggers) {
            logger.logEvent(event, eventType);
        }
    }
}
