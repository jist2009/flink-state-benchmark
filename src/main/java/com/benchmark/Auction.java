package com.benchmark;

import java.io.Serializable;

public class Auction implements Serializable {
    public long id;
    public long seller; // Clé de jointure avec Person.id
    public double reserve;
    public long timestamp;

    public Auction() {}

    public Auction(long id, long seller, double reserve, long timestamp) {
        this.id = id;
        this.seller = seller;
        this.reserve = reserve;
        this.timestamp = timestamp;
    }
}