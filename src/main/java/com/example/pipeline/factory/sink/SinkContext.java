package com.example.pipeline.factory.sink;

public interface SinkContext<T> {
    void init();
    void receive(T item);
    void close();
}