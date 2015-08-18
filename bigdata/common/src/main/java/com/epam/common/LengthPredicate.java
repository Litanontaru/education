package com.epam.common;

public class LengthPredicate {
    public boolean test(String s) {
        return s != null && s.length() > 3;
    }
}
