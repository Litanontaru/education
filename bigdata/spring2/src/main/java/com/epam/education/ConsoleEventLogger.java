package com.epam.education;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 9:34 AM
 */
public class ConsoleEventLogger implements EventLogger {
    public void logEvent(String msg) {
        System.out.println(msg);
    }
}
