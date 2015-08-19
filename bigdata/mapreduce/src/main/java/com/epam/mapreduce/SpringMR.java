package com.epam.mapreduce;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringMR {
    public static void main(String[] arguments) {
        new ClassPathXmlApplicationContext("META-INF/spring/boot-application-context.xml");
    }
}
