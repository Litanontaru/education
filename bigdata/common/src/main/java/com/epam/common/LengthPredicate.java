package com.epam.common;

import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class LengthPredicate implements Serializable {
    private static final long serialVersionUID = -3107518506312893053L;

    public boolean test(String s) {
        return s != null && s.length() > 3;
    }
}
