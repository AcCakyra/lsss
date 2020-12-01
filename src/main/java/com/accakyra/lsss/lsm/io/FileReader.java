package com.accakyra.lsss.lsm.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileReader {

    public static ByteBuffer read(Path fileName) throws IOException {
        try (FileChannel channel = FileChannel.open(fileName, StandardOpenOption.READ)) {
            return readFileChannel(channel, 0, (int) channel.size());
        }
    }

    public static ByteBuffer read(Path fileName, int offset, int length) throws IOException {
        try (FileChannel channel = FileChannel.open(fileName, StandardOpenOption.READ)) {
            return readFileChannel(channel, offset, length);
        }
    }

    private static ByteBuffer readFileChannel(FileChannel channel, int offset, int length) throws IOException {
        channel.position(offset);
        ByteBuffer buffer = ByteBuffer.allocateDirect(length);
        while (buffer.position() < buffer.limit()) {
            int bytes = channel.read(buffer);
            if (bytes <= 0) break;
        }
        buffer.flip();
        return buffer;
    }
}
