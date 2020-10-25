package com.accakyra.lsss.lsm.store;

import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaData implements Closeable {

    private final File data;
    private AtomicInteger sstGeneration;
    private AtomicInteger indexGeneration;

    public MetaData(File data) {
        this.data = data;
        int generation = open(data);
        this.sstGeneration = new AtomicInteger(generation);
        this.indexGeneration = new AtomicInteger(generation);
    }

    public int open(File data) {
        try {
            String metadataFileName = FileNameUtil.buildMetaDataFileName(data.getAbsolutePath());
            File metadata = new File(metadataFileName);
            if (metadata.exists()) {
                DataInputStream input = new DataInputStream(new FileInputStream(metadata));
                return input.readInt();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int getSstGeneration() {
        return sstGeneration.get();
    }

    public int getIndexGeneration() {
        return indexGeneration.get();
    }

    public int getAndIncrementIndexGeneration() {
        return indexGeneration.getAndIncrement();
    }

    public void incrementSstGeneration() {
        sstGeneration.incrementAndGet();
        flush();
    }

    @Override
    public void close() {
        flush();
    }

    private void flush() {
        String metadataFileName = FileNameUtil.buildMetaDataFileName(data.getAbsolutePath());
        try (DataOutputStream output = new DataOutputStream(new FileOutputStream(metadataFileName))) {
            output.writeInt(sstGeneration.get());
            output.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
