package com.accakyra.lsss.lsm.data.memory;

import java.nio.ByteBuffer;

public class SnapshotKey implements Comparable<SnapshotKey> {

    private final ByteBuffer key;
    private final int snapshot;

    public SnapshotKey(ByteBuffer key, int snapshot) {
        this.key = key;
        this.snapshot = snapshot;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public int getSnapshot() {
        return snapshot;
    }

    @Override
    public int compareTo(SnapshotKey o) {
        int compare = key.compareTo(o.getKey());
        if (compare == 0) {
            return o.getSnapshot() - snapshot;
        } else {
            return compare;
        }
    }
}