package com.accakyra.lsss;

public class Config {

    public static class Builder {

        private int memtableSize = 16 * 1024 * 1024;
        private int maxImmtableCount = 10;
        private int fanout = 10;
        private int sparseStep = 10;
        private int maxKeySize = 16;

        public Builder memtableSize(int memtableSize) {
            this.memtableSize = memtableSize;
            return this;
        }

        public Builder maxImmtableCount(int maxImmtableCount) {
            this.maxImmtableCount = maxImmtableCount;
            return this;
        }

        public Builder fanout(int fanout) {
            this.fanout = fanout;
            return this;
        }

        public Builder sparseStep(int sparseStep) {
            this.sparseStep = sparseStep;
            return this;
        }

        public Builder maxKeySize(int maxKeySize) {
            this.maxKeySize = maxKeySize;
            return this;
        }

        public Config build() {
            return new Config(memtableSize, maxImmtableCount, fanout, sparseStep, maxKeySize);
        }
    }

    /**
     * Maximum possible size of memtable before flushing
     * on persistence storage.
     */
    private int memtableSize;

    /**
     * Maximum possible count of immtables storing in memory.
     */
    private int maxImmtableCount;

    /**
     * The size ratio of adjacent levels.
     */
    private int fanout;

    /**
     * Count of k/v pairs indexed by one value in sparse index.
     */
    private int sparseStep;

    /**
     * Max count of bytes for single key in spars index;
     */
    private int maxKeySize;

    public Config(int memtableSize, int maxImmtableCount, int fanout, int sparseStep, int maxKeySize) {
        this.memtableSize = memtableSize;
        this.maxImmtableCount = maxImmtableCount;
        this.fanout = fanout;
        this.sparseStep = sparseStep;
        this.maxKeySize = maxKeySize;
    }

    public int getMemtableSize() {
        return memtableSize;
    }

    public int getMaxImmtableCount() {
        return maxImmtableCount;
    }

    public int getFanout() {
        return fanout;
    }

    public int getSparseStep() {
        return sparseStep;
    }

    public int getMaxKeySize() {
        return maxKeySize;
    }
}
