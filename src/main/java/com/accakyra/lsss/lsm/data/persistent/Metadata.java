package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.lsm.io.FileReader;
import com.accakyra.lsss.lsm.io.FileWriter;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class Metadata implements Closeable {

    private final AtomicInteger maxTableId;
    private final Path fileName;

    public Metadata(File data) {
        fileName = FileNameUtil.buildMetaDataFileName(data.toPath());
        maxTableId = new AtomicInteger(readTableId());
    }

    public int readTableId() {
        if (Files.exists(fileName)) {
            ByteBuffer idBuffer = FileReader.read(fileName);
            if (idBuffer != null) {
                return idBuffer.getInt();
            }
        }
        return 0;
    }

    public int getAndIncrementTableId() {
        return maxTableId.getAndIncrement();
    }

    @Override
    public void close() {
        flush();
    }

    private void flush() {
        ByteBuffer idBuffer = ByteBuffer.allocate(4);
        idBuffer.putInt(maxTableId.get());
        idBuffer.flip();
        FileWriter.write(fileName, idBuffer);
    }
}
