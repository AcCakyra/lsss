package com.accakyra.lsss.lsm.data.persistent.level;

import com.accakyra.lsss.lsm.Config;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.io.read.TableReader;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.io.File;
import java.util.*;

public class Levels {

    private final Map<Integer, Level> levels;
    private final Map<Integer, Memtable> immtables;

    public Levels(File data) {
        this.levels = TableReader.readLevels(data);
        this.immtables = new TreeMap<>(Comparator.reverseOrder());
    }

    public List<Resource> getResources() {
        List<Resource> levelsList = new ArrayList<>();
        levelsList.addAll(immtables.values());
        levelsList.addAll(levels.values());
        return levelsList;
    }

    public Level getLevel(int level) {
        if (levels.containsKey(level)) {
            return levels.get(level);
        }
        return null;
    }

    public void addImmtable(int id, Memtable immtable) {
        immtables.put(id, immtable);
    }

    public void deleteImmtable(int id) {
        immtables.remove(id);
    }

    public void addSST(int level, SST sst) {
        if (levels.containsKey(level)) {
            levels.get(level).add(sst);
        }
    }

    public int levelOverflow(int level) {
        return levels.get(level).size() - (int) Math.pow(Config.FANOUT, level + 1);
    }

    public void resetZeroLevel() {
        levels.put(0, new Level0());
    }

    public void addLevel(int level) {
        if (!levels.containsKey(level)) {
            levels.put(level, new LevelN());
        }
    }
}
