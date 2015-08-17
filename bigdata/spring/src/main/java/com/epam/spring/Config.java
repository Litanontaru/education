package com.epam.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {
    private static Logger logger = LoggerFactory.getLogger(Config.class);

    @Bean
    public Girl kleo() {
        return new Girl();
    }

    @Bean
    public Intelligence mind() {
        return new Intelligence();
    }

    public static void main(String[] args) {
        logger.debug("Start");
        ApplicationContext context = null;
        try {
            context = new AnnotationConfigApplicationContext(Config.class);
            context.getBean(Girl.class).think();
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}
