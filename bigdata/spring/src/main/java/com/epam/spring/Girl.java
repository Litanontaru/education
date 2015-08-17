package com.epam.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Andrei_Yakushin on 8/14/2015.
 */
public class Girl {
    private Intelligence intelligence;

    public Girl() {
    }

    public Girl(Intelligence intelligence) {
        this.intelligence = intelligence;
    }

    @Autowired
    public void setIntelligence(Intelligence intelligence) {
        this.intelligence = intelligence;
    }

    public void think() {
        intelligence.think();
    }

    //------------------------------------------------------------------------------------------------------------------

    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext("kleo.xml");
        Girl kleo = (Girl) context.getBean("kleo");
        kleo.think();
    }
}
