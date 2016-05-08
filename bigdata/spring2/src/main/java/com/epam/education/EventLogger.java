package com.epam.education;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 9:45 AM
 */
public interface EventLogger {
    void logEvent(Event event, EventType eventType);
}
