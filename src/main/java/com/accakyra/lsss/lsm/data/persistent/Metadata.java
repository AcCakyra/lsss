package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.lsm.io.FileReader;
import com.accakyra.lsss.lsm.io.FileWriter;
import com.accakyra.lsss.lsm.util.FileNameUtil;
import lombok.extern.java.Log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

@Log
public class Metadata implements Closeable {

    private final AtomicInteger maxTableId;
    private final Path fileName;

    public Metadata(File data) {
        fileName = FileNameUtil.buildMetaDataFileName(data.toPath());
        maxTableId = new AtomicInteger(readTableId());
    }

    public int readTableId() {
        if (Files.exists(fileName)) {
            try {
                ByteBuffer idBuffer = FileReader.read(fileName);
                return idBuffer.getInt();
            } catch (IOException e) {
                log.log(Level.SEVERE, "Cannot read metadata from file", e);
                throw new RuntimeException(e);
            }
        } else {
            return 0;
        }
    }

    public int getAndIncrementTableId() {
        return maxTableId.getAndIncrement();
    }

    @Override
    public void close() {
        flush();
    }

    private void flush() {
        try {
            ByteBuffer idBuffer = ByteBuffer.allocate(4);
            idBuffer.putInt(maxTableId.get());
            idBuffer.flip();
            FileWriter.write(fileName, idBuffer);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Cannot flush metadata to file", e);
            throw new RuntimeException(e);
        }
    }
}
