package com.accakyra.lsss.lsm;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.read.FileReader;
import com.accakyra.lsss.lsm.io.write.TableWriter;
import com.accakyra.lsss.lsm.storage.*;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TableManager {

    private final File data;
    private final TableWriter writer;

    public TableManager(File data) {
        this.data = data;
        this.writer = new TableWriter();
    }

    public void flush(Level level, ReentrantReadWriteLock lock, Memtable memtable, int tableId) {
        // Overall size of index file is:
        // 4 bytes for level
        // + (4 bytes for storing length of key
        //    + 4 bytes for storing offset in sst file for value of this key
        //    + 4 bytes for storing length of value
        //   ) * count of keys
        // + size of all keys
        int keysInfoSize = (4 + 4 + 4) * memtable.getUniqueKeysCount();
        int indexSize = 4 + memtable.getKeysCapacity() + keysInfoSize;
        int memtableSize = memtable.getTotalBytesCapacity();

        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        ByteBuffer sstBuffer = ByteBuffer.allocate(memtableSize);

        NavigableMap<ByteBuffer, KeyInfo> indexKeys = new TreeMap<>();

        indexBuffer.putInt(0);

        int valueOffset = 0;
        for (Record record : memtable) {
            byte[] key = record.getKey().array();
            byte[] value = record.getValue().array();

            sstBuffer.put(key);
            sstBuffer.put(value);

            valueOffset += key.length;
            indexBuffer.putInt(key.length);
            indexBuffer.put(key);
            indexBuffer.putInt(valueOffset);
            indexBuffer.putInt(value.length);
            indexKeys.put(record.getKey(), new KeyInfo(valueOffset, value.length));
            valueOffset += value.length;
        }

        sstBuffer.flip();
        indexBuffer.flip();
        Index index = new Index(0, indexKeys);

        writer.flushMemtable(sstBuffer, indexBuffer, tableId, data.toPath(), level, index, lock);
    }

    public Map<Integer, Level> readLevels() {
        Map<Integer, List<SST>> sstMap = readSSTs()
                .stream()
                .collect(Collectors.groupingBy(SST::getLevel));

        Map<Integer, Level> levels = new HashMap<>();
        for (Map.Entry<Integer, List<SST>> ssts : sstMap.entrySet()) {
            int level = ssts.getKey();
            List<SST> sstList = ssts.getValue();
            sstList.sort(Comparator.comparingInt(SST::getId).reversed());
            levels.put(level, new Level(sstList));
        }
        return levels;
    }

    private List<SST> readSSTs() {
        List<Integer> tableIds = findAllTableIds();
        List<SST> ssts = new ArrayList<>();
        for (int id : tableIds) {
            Path indexFileName = FileNameUtil.buildIndexFileName(data.toPath(), id);
            Path sstFileName = FileNameUtil.buildSstableFileName(data.toPath(), id);
            Index index = parseIndex(FileReader.read(indexFileName));
            SST sst = new SST(index, id, sstFileName);
            ssts.add(sst);
        }
        return ssts;
    }

    private List<Integer> findAllTableIds() {
        return Arrays.stream(data.listFiles())
                .map(File::getName)
                .filter(FileNameUtil::isSstableFileName)
                .map(name -> name.replaceAll("[^0-9]", ""))
                .mapToInt(FileNameUtil::extractIdFormSstFileName)
                .boxed()
                .collect(Collectors.toList());
    }

    private Index parseIndex(ByteBuffer buffer) {
        NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();
        int level = buffer.getInt();
        while (buffer.hasRemaining()) {
            int keySize = buffer.getInt();
            byte[] key = new byte[keySize];
            buffer.get(key);
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            int offset = buffer.getInt();
            int valueSize = buffer.getInt();
            keys.put(keyBuffer, new KeyInfo(offset, valueSize));
        }
        return new Index(level, keys);
    }

    public void close() {
        writer.close();
    }
}
