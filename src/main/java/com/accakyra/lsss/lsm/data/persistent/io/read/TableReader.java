package com.accakyra.lsss.lsm.data.persistent.io.read;

import com.accakyra.lsss.Config;
import com.accakyra.lsss.lsm.data.persistent.level.Level;
import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.data.persistent.level.Level0;
import com.accakyra.lsss.lsm.data.persistent.level.LevelN;
import com.accakyra.lsss.lsm.data.persistent.sst.KeyInfo;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.io.FileReader;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class TableReader {

    public static Map<Integer, Level> readLevels(File data, Config config) {
        Map<Integer, List<SST>> sstableMap =
                readSSTables(data, config)
                        .stream()
                        .collect(Collectors.groupingBy(SST::getLevel));

        Map<Integer, Level> levels = new HashMap<>();
        int maxLevelNumber = 0;
        for (Map.Entry<Integer, List<SST>> sstables : sstableMap.entrySet()) {
            int levelNumber = sstables.getKey();
            maxLevelNumber = Math.max(maxLevelNumber, levelNumber);

            Level level;
            if (levelNumber == 0) level = new Level0();
            else level = new LevelN();

            for (SST sst : sstables.getValue()) level.add(sst);
            levels.put(levelNumber, level);
        }

        for (int levelNumber = 0; levelNumber <= maxLevelNumber; levelNumber++) {
            if (!levels.containsKey(levelNumber)) {
                Level level;
                if (levelNumber == 0) level = new Level0();
                else level = new LevelN();
                levels.put(levelNumber, level);
            }
        }
        return levels;
    }

    private static List<SST> readSSTables(File data, Config config) {
        List<Integer> tableIds = findAllTableIds(data);
        List<SST> ssts = new ArrayList<>();
        for (int id : tableIds) {
            Path indexFileName = FileNameUtil.buildIndexFileName(data.toPath(), id);
            Path sstFileName = FileNameUtil.buildSSTableFileName(data.toPath(), id);
            ByteBuffer indexBuffer = FileReader.read(indexFileName);
            int level = indexBuffer.getInt();
            NavigableMap<ByteBuffer, KeyInfo> index = TableConverter.parseIndexBufferSparse(
                    indexBuffer, config.getMaxKeySize(), config.getSparseStep());
            ssts.add(new SST(index, id, sstFileName, indexFileName, level));
        }
        return ssts;
    }

    private static List<Integer> findAllTableIds(File data) {
        return Arrays.stream(data.listFiles())
                .map(File::getName)
                .filter(FileNameUtil::isIndexFileName)
                .mapToInt(FileNameUtil::extractIdFormIndexFileName)
                .boxed()
                .collect(Collectors.toList());
    }
}
