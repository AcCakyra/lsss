package com.accakyra.lsss;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface DAO extends Closeable {

    void upsert(ByteBuffer key, ByteBuffer value);

    ByteBuffer get(ByteBuffer key);

    void delete(ByteBuffer key);
}
