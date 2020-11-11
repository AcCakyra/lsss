package com.accakyra.lsss.lsm.data.io.read;

import com.accakyra.lsss.lsm.data.persistent.Level;
import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.data.persistent.sst.Index;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.io.FileReader;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class TableReader {

    public Map<Integer, Level> readLevels(File data) {
        Map<Integer, List<SST>> sstMap = readSSTs(data)
                .stream()
                .collect(Collectors.groupingBy(SST::getLevel));

        Map<Integer, Level> levels = new HashMap<>();
        for (Map.Entry<Integer, List<SST>> ssts : sstMap.entrySet()) {
            int level = ssts.getKey();
            List<SST> sstList = ssts.getValue();
            sstList.sort(Comparator.comparingInt(SST::getId).reversed());
            levels.put(level, new Level(sstList));
        }

        if (levels.isEmpty()) {
            levels.put(0, new Level());
        }
        return levels;
    }

    private List<SST> readSSTs(File data) {
        List<Integer> tableIds = findAllTableIds(data);
        List<SST> ssts = new ArrayList<>();
        for (int id : tableIds) {
            Path indexFileName = FileNameUtil.buildIndexFileName(data.toPath(), id);
            Path sstFileName = FileNameUtil.buildSstableFileName(data.toPath(), id);
            Index index = TableConverter.parseIndexBuffer(FileReader.read(indexFileName));
            SST sst = new SST(index, id, sstFileName);
            ssts.add(sst);
        }
        return ssts;
    }

    private List<Integer> findAllTableIds(File data) {
        return Arrays.stream(data.listFiles())
                .map(File::getName)
                .filter(FileNameUtil::isSstableFileName)
                .map(name -> name.replaceAll("[^0-9]", ""))
                .mapToInt(FileNameUtil::extractIdFormSstFileName)
                .boxed()
                .collect(Collectors.toList());
    }
}
