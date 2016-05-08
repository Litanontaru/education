package com.epam.education;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author Andrei_Yakushin
 * @since 5/8/2016 10:31 AM
 */
public class FileEventLogger implements EventLogger {
    private String filename;
    private File file;

    public FileEventLogger(String filename) {
        this.filename = filename;
    }

    public void init() {
        file = new File(filename);
        if (file.canWrite()) {
            throw new IllegalStateException();
        }
    }

    public void logEvent(Event event, EventType eventType) {
        try {
            FileUtils.writeStringToFile(file, event.toString());
        } catch (IOException e) {
            //do nothing
        }
    }
}
