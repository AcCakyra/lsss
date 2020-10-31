package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.store.Index;
import com.accakyra.lsss.lsm.store.KeyInfo;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class SSTReader {

    public static ByteBuffer getValueFromStorage(File data, KeyInfo keyInfo, int generation) {
        String sstFileName = FileNameUtil.buildSstableFileName(data.getAbsolutePath(), generation);

        try (RandomAccessFile indexFile = new RandomAccessFile(sstFileName, "r");
             FileChannel channel = indexFile.getChannel()) {

            MappedByteBuffer dataBuffer = channel.map(FileChannel.MapMode.READ_ONLY, keyInfo.getOffset(), keyInfo.getValueSize());
            byte[] value = new byte[keyInfo.getValueSize()];
            dataBuffer.get(value);
            return ByteBuffer.wrap(value);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
