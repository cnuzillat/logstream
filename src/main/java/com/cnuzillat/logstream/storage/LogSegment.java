public class LogSegment {
    private final FileChannel channel;
    private long nextOffset = 0;

    public LogSegment(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
    }
}