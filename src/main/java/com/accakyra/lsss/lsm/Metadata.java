
package com.accakyra.lsss.lsm;

import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.*;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class Metadata implements Closeable {

    private final File data;
    private AtomicInteger tableId;

    public Metadata(File data) {
        this.data = data;
        this.tableId = new AtomicInteger(open(data));
    }

    public int open(File data) {
        try {
            Path metadataFileName = FileNameUtil.buildMetaDataFileName(data.toPath());
            File metadata = new File(metadataFileName.toString());
            if (metadata.exists()) {
                DataInputStream input = new DataInputStream(new FileInputStream(metadata));
                return input.readInt();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int getAndIncrementTableId() {
        return tableId.getAndIncrement();
    }

    @Override
    public void close() {
        flush();
    }

    private void flush() {
        Path metadataFileName = FileNameUtil.buildMetaDataFileName(data.toPath());
        try (DataOutputStream output = new DataOutputStream(new FileOutputStream(metadataFileName.toString()))) {
            output.writeInt(tableId.get());
            output.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
