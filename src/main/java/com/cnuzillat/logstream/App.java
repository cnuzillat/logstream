package com.cnuzillat.logstream;

import com.cnuzillat.logstream.storage.DurabilityMode;
import com.cnuzillat.logstream.storage.LogSegment;

import java.nio.file.Path;
import java.nio.file.Paths;

public class App {

    public static void main(String[] args) throws Exception {
        Path path = Paths.get("segment.log");
        LogSegment segment = new LogSegment(path, DurabilityMode.NO_FLUSH);
        System.out.println(segment.readFromOffset(100000));
    }
}