package com.cnuzillat.logstream;

import com.cnuzillat.logstream.storage.LogSegment;

import java.nio.file.Path;

public class App {
    public static void main(String[] args) throws Exception {
        LogSegment segment = new LogSegment(Path.of("data.log"));
        segment.append("hello".getBytes());
        segment.append("world".getBytes());
        System.out.println(segment.readAll());
    }
}