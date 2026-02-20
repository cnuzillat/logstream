package com.cnuzillat.logstream.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class LogSegment {
    private final FileChannel channel;
    private long nextOffset = 0;
    private final DurabilityMode durabilityMode;
    private final NavigableMap<Long, Long> index;
    final long FLUSH_INTERVAL_NANOS = 1_000_000_000L;
    private long lastFlushTime = 0;

    public LogSegment(Path path, DurabilityMode durabilityMode) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        this.durabilityMode = durabilityMode;
        index = new TreeMap<Long, Long>();
        recoverOffset();
    }

    private void recoverOffset() throws IOException {
        channel.position(0);

        ByteBuffer header = ByteBuffer.allocate(12);
        long lastOffset = -1;

        while (true) {
            long recordStartPosition = channel.position();

            header.clear();
            while (header.hasRemaining()) {
                if (channel.read(header) == -1) {
                    break;
                }
            }

            if (header.position() < 12) {
                break;
            }

            header.flip();
            int length = header.getInt();
            long offset = header.getLong();

            long remainingFileBytes = channel.size() - channel.position();

            if (remainingFileBytes < length) {
                channel.truncate(recordStartPosition);
                channel.force(true);
                channel.position(recordStartPosition);
                break;
            }

            channel.position(channel.position() + length);

            lastOffset = offset;

            if (offset % 10 == 0) {
                index.put(offset, recordStartPosition);
            }
        }
        nextOffset = lastOffset + 1;

        channel.position(channel.size());
    }

    public synchronized long append(byte[] payload) throws IOException {
        long currentOffset = nextOffset++;

        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + payload.length);

        buffer.putInt(payload.length);
        buffer.putLong(currentOffset);
        buffer.put(payload);

        buffer.flip();

        long recordStartPosition = channel.position();
        if (currentOffset % 10 == 0) {
            index.put(currentOffset, recordStartPosition);
        }

        while (buffer.hasRemaining()) {
            int bytesWritten = channel.write(buffer);
            if (bytesWritten == 0) {
                Thread.yield();
            }
        }

        switch (durabilityMode) {
            case FORCE_EVERY_APPEND:
                channel.force(false);
                break;
            case PERIODIC_FLUSH:
                long now = System.nanoTime();
                if (now - lastFlushTime > FLUSH_INTERVAL_NANOS) {
                    channel.force(false);
                    lastFlushTime = now;
                }
                break;
            case NO_FLUSH:
                break;
        }

        return currentOffset;
    }

    public List<String> readAll() throws IOException {
        channel.position(0);

        List<String> records = new ArrayList<>();

        ByteBuffer header = ByteBuffer.allocate(12);

        while (true) {
            header.clear();

            while (header.hasRemaining()) {
                int bytesRead = channel.read(header);
                if (bytesRead == -1) {
                    return records;
                }
            }
            header.flip();

            int length = header.getInt();
            long offset = header.getLong();

            if (length < 0 || length > channel.size() - channel.position()) {
                return records;
            }

            ByteBuffer payloadBuffer = ByteBuffer.allocate(length);

            while (payloadBuffer.hasRemaining()) {
                int bytesRead = channel.read(payloadBuffer);
                if (bytesRead == -1) {
                    return records;
                }
            }

            payloadBuffer.flip();

            byte[] payload = new byte[length];
            payloadBuffer.get(payload);
            records.add(offset + ": " + new String(payload));
        }
    }

    public String readFromOffset(long targetOffset) throws IOException {
        Long floor = index.floorKey(targetOffset);

        if (floor == null) {
            channel.position(0);
        }
        else {
            channel.position(index.get(floor));
        }

        ByteBuffer header = ByteBuffer.allocate(12);

        while (true) {
            header.clear();

            while (header.hasRemaining()) {
                int bytesRead = channel.read(header);
                if (bytesRead == -1) {
                    return null;
                }
            }

            header.flip();
            int length = header.getInt();
            long offset = header.getLong();

            if (length < 0 || length > channel.size() - channel.position()) {
                return null;
            }

            ByteBuffer payloadBuffer = ByteBuffer.allocate(length);
            while (payloadBuffer.hasRemaining()) {
                int bytesRead = channel.read(payloadBuffer);
                if (bytesRead == -1) {
                    return null;
                }
            }

            payloadBuffer.flip();
            byte[] payload = new byte[length];
            payloadBuffer.get(payload);

            if (offset == targetOffset) {
                return offset + ": " + new String(payload);
            }

            if (offset > targetOffset) {
                return null;
            }
        }
    }
}