package com.epam.mapreduce;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringContainer {
    private static ClassPathXmlApplicationContext springContext;
    private static final Object monitor = new Object();

    public static ClassPathXmlApplicationContext getContext() {
        if (springContext == null) {
            synchronized (monitor) {
                if (springContext == null) {
                    springContext = new ClassPathXmlApplicationContext("META-INF/spring/application-context.xml");
                }
            }
        }
        return springContext;
    }

    public static void requestInjections(Object bean) {
        getContext().getAutowireCapableBeanFactory().autowireBean(bean);
    }
}
