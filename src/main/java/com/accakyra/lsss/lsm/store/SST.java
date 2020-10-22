package com.accakyra.lsss.lsm.store;

import java.nio.ByteBuffer;

public class SST {

    private final ByteBuffer sstable;
    private final ByteBuffer index;

    public SST(ByteBuffer sstable, ByteBuffer index) {
        this.sstable = sstable;
        this.index = index;
    }

    public ByteBuffer getIndex() {
        return index;
    }

    public ByteBuffer getSstable() {
        return sstable;
    }
}
