package com.cnuzillat.logstream.storage;

/**
 * Defines durability guarantees for log appends.
 * FORCE_EVERY_APPEND - Calls FileChannel.force(...) after each append.
 * PERIODIC_FLUSH     - Flushes periodically or in batches (not yet implemented).
 * NO_FLUSH           - Relies entirely on OS page cache (no explicit flush).
 */
public enum DurabilityMode {
    FORCE_EVERY_APPEND,
    PERIODIC_FLUSH,
    NO_FLUSH,
}
