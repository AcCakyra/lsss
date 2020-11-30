package com.accakyra.lsss;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Config {

    /**
     * Maximum possible size of memtable before flushing
     * on persistence storage.
     */
    @Builder.Default
    private final int memtableSize = 16 * 1024 * 1024; //16 MB

    /**
     * Maximum possible count of immtables storing in memory.
     */
    @Builder.Default
    private final int maxImmtableCount = 10;

    /**
     * The size ratio of adjacent levels.
     */
    @Builder.Default
    private final int fanout = 10;

    /**
     * Count of k/v pairs indexed by one value in sparse index.
     */
    @Builder.Default
    private final int sparseStep = 10;

    /**
     * Max count of bytes for single key in spars index;
     */
    @Builder.Default
    private final int maxKeySize = 16;
}
