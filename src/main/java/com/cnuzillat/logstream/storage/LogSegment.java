public class LogSegment {
    private final FileChannel channel;
    private long nextOffset = 0;

    public LogSegment(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
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
}