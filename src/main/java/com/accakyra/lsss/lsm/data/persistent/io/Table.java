package com.accakyra.lsss.lsm.data.persistent.io;

import java.nio.ByteBuffer;

public class Table {

    private final ByteBuffer indexBuffer;
    private final ByteBuffer sstBuffer;
    private final int id;

    public Table(ByteBuffer indexBuffer, ByteBuffer sstBuffer, int id) {
        this.indexBuffer = indexBuffer;
        this.sstBuffer = sstBuffer;
        this.id = id;
    }

    public ByteBuffer getIndexBuffer() {
        return indexBuffer;
    }

    public ByteBuffer getSstBuffer() {
        return sstBuffer;
    }

    public int getId() {
        return id;
    }
}
