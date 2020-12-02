package com.accakyra.lsss.lsm.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

public class FileWriter {

    private static final Set<StandardOpenOption> writeOptions = Set.of(
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            StandardOpenOption.CREATE);

    public static void write(Path fileName, ByteBuffer content) throws IOException {
        try (FileChannel channel = FileChannel.open(fileName, writeOptions)) {
            while (content.hasRemaining()) {
                int bytes = channel.write(content);
                if (bytes <= 0) break;
            }
            channel.force(true);
        }
    }
}
