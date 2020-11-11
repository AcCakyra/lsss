package com.accakyra.lsss.lsm.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

public class FileWriter {

    public static void write(Path fileName, ByteBuffer content) {
        Set<StandardOpenOption> options = Set.of(StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE);
        int position = 0;
        int length = content.capacity();

        try (FileChannel channel = FileChannel.open(fileName, options)) {
            MappedByteBuffer mapBuffer = channel.map(FileChannel.MapMode.READ_WRITE, position, length);
            mapBuffer.put(content);
            mapBuffer.force();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
