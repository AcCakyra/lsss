package com.accakyra.lsss.lsm.io.read;

import com.accakyra.lsss.lsm.storage.KeyInfo;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class FileReader {

    public static ByteBuffer readValue(String fileName, KeyInfo keyInfo) {
        try (RandomAccessFile indexFile = new RandomAccessFile(fileName, "r");
             FileChannel channel = indexFile.getChannel()) {
            int offset = keyInfo.getOffset();
            int bytesToRead = keyInfo.getValueSize();
            MappedByteBuffer dataBuffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, bytesToRead);
            byte[] value = new byte[bytesToRead];
            dataBuffer.get(value);
            return ByteBuffer.wrap(value);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
