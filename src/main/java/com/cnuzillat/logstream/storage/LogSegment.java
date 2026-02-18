package com.cnuzillat.logstream.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogSegment {
    private final FileChannel channel;
    private long nextOffset = 0;

    public LogSegment(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

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
        }

        nextOffset = lastOffset + 1;
    }

    public synchronized long append(byte[] payload) throws IOException {
        long currentOffset = nextOffset++;

        ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + payload.length);

        buffer.putInt(payload.length);
        buffer.putLong(currentOffset);
        buffer.put(payload);

        buffer.flip();

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        return currentOffset;
    }

    public List<String> readAll() throws IOException {
        channel.position(0);

        List<String> records = new ArrayList<>();

        ByteBuffer header = ByteBuffer.allocate(12);

        while (true) {
            header.clear();

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
}