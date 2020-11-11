package com.accakyra.lsss.lsm.data.persistent.sst;

import java.nio.ByteBuffer;
import java.util.NavigableMap;

public class Table {

    private ByteBuffer indexBuffer;
    private ByteBuffer sstBuffer;
    private NavigableMap<ByteBuffer, KeyInfo> keysInfo;

    public Table(ByteBuffer indexBuffer, ByteBuffer sstBuffer, NavigableMap<ByteBuffer, KeyInfo> keysInfo) {
        this.indexBuffer = indexBuffer;
        this.sstBuffer = sstBuffer;
        this.keysInfo = keysInfo;
    }

    public ByteBuffer getIndexBuffer() {
        return indexBuffer;
    }

    public ByteBuffer getSstBuffer() {
        return sstBuffer;
    }

    public NavigableMap<ByteBuffer, KeyInfo> getKeysInfo() {
        return keysInfo;
    }
}
