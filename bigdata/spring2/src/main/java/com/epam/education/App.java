package com.epam.education;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 9:34 AM
 */
public class App {
    private Client client;
    private Map<EventType, EventLogger> loggers;
    private EventLogger defaultLogger;

    public App(Client client, Map<EventType, EventLogger> loggers) {
        this.client = client;
        this.loggers = loggers;
    }

    public void setDefaultLogger(EventLogger defaultLogger) {
        this.defaultLogger = defaultLogger;
    }

    public void logEvent(Event event, EventType eventType) {
        if (eventType == null) {
            defaultLogger.logEvent(event, eventType);
        } else {
            loggers.get(eventType).logEvent(event, eventType);
        }
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("application-context.xml");
        App app = ctx.getBean(App.class);

        System.out.println(ctx.getBean(Event.class));
        System.out.println(ctx.getBean(Event.class));
        System.out.println(ctx.getBean(Event.class));

        ctx.close();
    }
}
