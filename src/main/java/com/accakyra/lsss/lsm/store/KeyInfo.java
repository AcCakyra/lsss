package com.accakyra.lsss.lsm.store;

public class KeyInfo {

    private int offset;
    private int valueSize;

    public KeyInfo(int offset, int valueSize) {
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public int getOffset() {
        return offset;
    }

    public int getValueSize() {
        return valueSize;
    }
}
