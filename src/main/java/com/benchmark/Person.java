package com.benchmark;

import java.io.Serializable;

public class Person implements Serializable {
    public long id;
    public String name;
    public long timestamp;

    public Person() {}

    public Person(long id, String name, long timestamp) {
        this.id = id;
        this.name = name;
        this.timestamp = timestamp;
    }
}