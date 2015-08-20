package com.epam.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.Serializable;

@Component
public class LengthPredicate implements Serializable {
    private static final long serialVersionUID = -3107518506312893053L;

    private final int length;

    @Inject
    public LengthPredicate(@Value("${mapper.min.word.length}") int length) {
        this.length = length;
    }

    public boolean test(String s) {
        return s != null && s.length() >= length;
    }
}
