package com.accakyra.lsss.lsm.data.persistent.sst;

public class KeyInfo {

    private final int indexOffset;
    private final int sstOffset;
    private final int keySize;
    private final int valueSize;

    public KeyInfo(int indexOffset, int sstOffset, int keySize, int valueSize) {
        this.indexOffset = indexOffset;
        this.sstOffset = sstOffset;
        this.keySize = keySize;
        this.valueSize = valueSize;
    }

    public int getIndexOffset() {
        return indexOffset;
    }

    public int getSstOffset() {
        return sstOffset;
    }

    public int getKeySize() {
        return keySize;
    }

    public int getValueSize() {
        return valueSize;
    }
}
