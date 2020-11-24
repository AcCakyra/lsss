package com.accakyra.lsss.lsm.data.persistent.io;

import java.nio.ByteBuffer;

public class Table {

    private final ByteBuffer indexBuffer;
    private final ByteBuffer sstBuffer;

    public Table(ByteBuffer indexBuffer, ByteBuffer sstBuffer) {
        this.indexBuffer = indexBuffer;
        this.sstBuffer = sstBuffer;
    }

    public ByteBuffer getIndexBuffer() {
        return indexBuffer;
    }

    public ByteBuffer getSstBuffer() {
        return sstBuffer;
    }
}
