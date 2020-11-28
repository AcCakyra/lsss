package com.accakyra.lsss.lsm.data.persistent.level;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.Config;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.io.read.TableReader;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

public class Levels {

    private final Map<Integer, Memtable> immtables;
    private final Map<Integer, Level> levels;

    public Levels(File data) {
        this.immtables = new TreeMap<>(Comparator.reverseOrder());
        this.levels = TableReader.readLevels(data);
    }

    public Record get(ByteBuffer key) {
        for (Resource immtable : immtables.values()) {
            Record record = immtable.get(key);
            if (record != null) return record;
        }
        for (Resource level : levels.values()) {
            Record record = level.get(key);
            if (record != null) return record;
        }
        return null;
    }

    public List<Resource> getResources() {
        List<Resource> resources = new ArrayList<>(immtables.values());
        resources.addAll(levels.values());
        return resources;
    }

    public void addImmtable(int id, Memtable immtable) {
        immtables.put(id, immtable);
    }

    public void deleteImmtable(int id) {
        immtables.remove(id);
    }

    public Level getLevel(int level) {
        return levels.get(level);
    }

    public void addLevel(int levelNumber, Level level) {
        levels.put(levelNumber, level);
    }

    public int levelOverflow(int level) {
        return levels.get(level).size() - (int) Math.pow(Config.FANOUT, level + 1);
    }
}
