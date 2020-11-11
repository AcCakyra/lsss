package com.accakyra.lsss.lsm.data.io;

import java.nio.ByteBuffer;

public class Table {

    private ByteBuffer indexBuffer;
    private ByteBuffer sstBuffer;
    private int id;

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
