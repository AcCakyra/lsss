package com.accakyra.lsss.lsm.data.memory;

import java.nio.ByteBuffer;

public class VersionedKey implements Comparable<VersionedKey> {

    private final ByteBuffer key;
    private final int version;

    public VersionedKey(ByteBuffer key, int version) {
        this.key = key;
        this.version = version;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public int compareTo(VersionedKey o) {
        int compare = key.compareTo(o.getKey());
        if (compare == 0) {
            return o.getVersion() - version;
        } else {
            return compare;
        }
    }
}