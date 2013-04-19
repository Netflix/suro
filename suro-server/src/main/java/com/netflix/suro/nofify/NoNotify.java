package com.netflix.suro.nofify;

public class NoNotify implements Notify {
    public static final String TYPE = "no";

    @Override
    public void init() {
    }

    @Override
    public boolean send(String message) {
        return true;
    }

    @Override
    public String recv() {
        return null;
    }

    @Override
    public String getStat() {
        return "No";
    }
}