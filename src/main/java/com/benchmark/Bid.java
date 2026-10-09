package com.benchmark;

import java.io.Serializable;

public class Bid implements Serializable, Timestamped{
    public long auction;   // identifiant de l'objet (clé de partitionnement)
    public double price;   // montant de l'enchère
    public long timestamp; // horodatage

    public Bid() {}

    public Bid(long auction, double price, long timestamp) {
        this.auction = auction;
        this.price = price;
        this.timestamp = timestamp;
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }
    
    @Override
    public String toString() {
        return "Bid{auction=" + auction + ", price=" + price + ", timestamp=" + timestamp + "}";
    }
}
