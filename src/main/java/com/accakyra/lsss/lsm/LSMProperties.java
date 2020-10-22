package com.accakyra.lsss.lsm;

import java.nio.ByteBuffer;

public class LSMProperties {
    /**
     * Maximum possible size of memtable before flushing
     * on persistence storage.
     */
    public final static int MEMTABLE_THRESHOLD = 4 * 1024 * 1024;

    /**
     * Special value for deleted keys
     */
    public static ByteBuffer TOMBSTONE = ByteBuffer.wrap("TTTT".getBytes());

}
