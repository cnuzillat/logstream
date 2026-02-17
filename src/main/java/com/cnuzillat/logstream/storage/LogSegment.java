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
            header.clear();
            int read = channel.read(header);
            if (read < 12) {
                break;
            }
            header.flip();
            int length = header.getInt();
            long offset = header.getLong();

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

        ByteBuffer header = ByteBuffer.allocate(4 + 8);

        while (true) {
            header.clear();

            int read = channel.read(header);
            if (read < 12) {
                break;
            }
            header.flip();

            int length = header.getInt();
            long offset = header.getLong();

            ByteBuffer payloadBuffer = ByteBuffer.allocate(length);
            channel.read(payloadBuffer);
            payloadBuffer.flip();

            byte[] payload = new byte[length];
            payloadBuffer.get(payload);
            records.add(offset + ": " + new String(payload));
        }
        return records;
    }
}