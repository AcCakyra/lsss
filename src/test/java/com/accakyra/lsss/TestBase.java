package com.accakyra.lsss;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

abstract class TestBase {
    private static final int KEY_LENGTH = 16;
    private static final int VALUE_LENGTH = 16;

    static ByteBuffer randomKey() {
        return randomBuffer(KEY_LENGTH);
    }

    static ByteBuffer randomValue() {
        return randomBuffer(VALUE_LENGTH);
    }

    static ByteBuffer randomBuffer(final int length) {
        final byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return ByteBuffer.wrap(result);
    }
}
