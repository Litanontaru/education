package com.epam.education;

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
        String message = msg.replaceAll(client.getId(), client.getFullName());
        eventLogger.logEvent(message);
    }

    public static void main(String[] args) {
//        App app = new App();
//        app.client = new Client("1", "John");
//        app.eventLogger = new ConsoleEventLogger();
    }
}
