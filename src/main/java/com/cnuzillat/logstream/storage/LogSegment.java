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
            records.add(payload + ": " + new String(payload));
        }
        return records;
    }
}