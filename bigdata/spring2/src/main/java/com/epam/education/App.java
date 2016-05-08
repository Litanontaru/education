package com.epam.education;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 9:34 AM
 */
public class App {
    private Client client;
    private EventLogger eventLogger;

    public App(Client client, EventLogger eventLogger) {
        this.client = client;
        this.eventLogger = eventLogger;
    }

    public void logEvent(String msg) {
//        String message = msg.replaceAll(client.getId(), client.getFullName());
//        eventLogger.logEvent(message);
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
