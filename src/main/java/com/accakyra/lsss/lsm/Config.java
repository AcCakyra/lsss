package com.accakyra.lsss.lsm;

public class Config {

    /**
     * Maximum possible size of memtable before flushing
     * on persistence storage.
     */
    public final static int MEMTABLE_THRESHOLD = 4 * 1024 * 1024;

    /**
     * Maximum possible count of immtables storing in memory.
     */
    public final static int MAX_MEMTABLE_COUNT = 10;

    /**
     * The size ratio of adjacent levels.
     */
    public final static int FANOUT = 4;

    /**
     * Count of k/v pairs indexed by one value in sparse index.
     */
    public final static int SPARSE_STEP = 10;

    /**
     * Max count of bytes for single key in spars index;
     */
    public final static int MAX_KEY_SIZE = 16;
}
