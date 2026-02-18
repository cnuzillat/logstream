package com.cnuzillat.logstream;

import com.cnuzillat.logstream.storage.DurabilityMode;
import com.cnuzillat.logstream.storage.LogSegment;

import java.nio.file.Files;
import java.nio.file.Path;

public class App {

    public static void main(String[] args) throws Exception {

        Path path = Path.of("data.log");

        Files.deleteIfExists(path);

        System.out.println("=== TEST 1: FORCE_EVERY_APPEND ===");
        LogSegment segment1 = new LogSegment(path, DurabilityMode.FORCE_EVERY_APPEND);
        long o1 = segment1.append("hello".getBytes());
        long o2 = segment1.append("world".getBytes());
        System.out.println("Offsets: " + o1 + ", " + o2);
        System.out.println("ReadAll: " + segment1.readAll());

        System.out.println("\n=== TEST 2: RESTART RECOVERY ===");

        LogSegment segment2 = new LogSegment(path, DurabilityMode.FORCE_EVERY_APPEND);
        long o3 = segment2.append("again".getBytes());
        System.out.println("Offset after restart: " + o3);
        System.out.println("ReadAll: " + segment2.readAll());

        System.out.println("\n=== TEST 3: NO_FLUSH ===");
        Files.deleteIfExists(path);
        LogSegment segment3 = new LogSegment(path, DurabilityMode.NO_FLUSH);
        segment3.append("A".getBytes());
        segment3.append("B".getBytes());
        System.out.println("ReadAll: " + segment3.readAll());

        System.out.println("\n=== TEST 4: PERIODIC_FLUSH (expected to throw) ===");
        Files.deleteIfExists(path);

        try {
            LogSegment segment4 = new LogSegment(path, DurabilityMode.PERIODIC_FLUSH);
            segment4.append("X".getBytes());
            System.out.println("ERROR: PERIODIC_FLUSH did NOT throw!");
        } catch (UnsupportedOperationException e) {
            System.out.println("PERIODIC_FLUSH correctly threw UnsupportedOperationException");
        }


        System.out.println("\nAll tests completed.");
    }
}