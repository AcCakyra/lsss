# lsss

lsss is yet another log-structured merge-tree implementation.

Note: It isn't production ready k/v storage, so don't use it in production.

## Implementation details 

### Write

All new updates handled by in-memory memtable. When memtable size become bigger
than the special threshold (16 MB by default) it replaced by new memtable 
and asynchronously flushes to disk.

### Read

If memtable contains record we are looking for we just return this record from memtable.
If not, lsss tries to find value in levels. Each level scanned throw all sparse indexes
to find one which might contain value. If some level has a sparse index which might contains record, 
lsss reads part of full index from disk, parse it, and if index contains the right key,
read value from sst file on disk.

### Deletion

Special value (tombstone) will be inserted for mark some key as deleted.
Tombstone will never be returned to client.

### Iteration

lsss support full and range scan (from (inclusive), to (not inclusive)).
Iterator represent view of database at some point of time
(moment between calling iterator and getting iterator back).
So, iterator will never see records inserted after it's creation.
This behavior is the reason why lsss provide closable iterator. It helps
keep in safety old sst files which unnecessary for work with new requests,
but probably contain records for iterator. So, don't forget to close it.

### Memtable

Memtable is just binary search tree. For support lsm functionality memtable store some metrics
about itself. Based on this metrics lsss understand when it has to be compacted.

### SST

SST is a bunch of sorted k/v pairs sotred on disk.

### Index

Index is bunch of sorted map (key -> info about key).
Main component of info is an offset of value in sst file.

Sparse index contains only part of real index. It helps decrease ram usage.

### Level

Level is bunch of sorted SST. Every level bigger than previous one in T(10 by default) times.
   
### Level-0

Level-0 contains new flushed SST. It works just like usual level but SST not sorted 
and might overlap each other.

### Compaction

When some level reach it's capacity (10 SST for level-0, 100 sst for level-1 and so on)
lsss start compaction process. In case of level-0 it collects all SST from level,
for any another it randomly collects SST while level will not fit in normal size.  
All collected SSTs merge with underlying level. So, underlying level might reaches its own capacity too 
and compaction will be called on this level too.

### Storage

All files stored into directory provided on creation of DAO instance. 
This directory represents persistent state of k/v storage.
If provided directory already has some files, lsss will try to read
it and use it for searching values for get operations.

### Performance

Note: Tests are were made depending on my naive understanding of OS and disk I/O 
and represent some metrics for quite small workload. So be carefully analyzing this.

Setup:
A million records per test. Each record has a 16 byte key, and a 100 byte value. 
       
    SSD:        SAMSUNG MZVLB512HAJQ-00000
    CPU:        Intel(R) Core(TM) i7-10510U CPU @ 1.80GHz
    Keys:       16 bytes each
    Values:     100 bytes each
    Records:    1000000
    Raw Size:   110.6 MB

See actual code in test/PerformanceTest

    Random read/write
    Read: 4.8 MB per second
    Write: 59 MB per second

## Example Usage

```java
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {

        // Insert and get
        try (DAO dao = DAOFactory.create(new File("/home/accakyra/temp/"))) {
            ByteBuffer key1 = stringToByteBuffer("key1");
            ByteBuffer key2 = stringToByteBuffer("key2");

            ByteBuffer value1 = stringToByteBuffer("value1");
            ByteBuffer value2 = stringToByteBuffer("value2");

            dao.upsert(key1, value1);
            dao.upsert(key2, value2);

            System.out.println(byteBufferToString(dao.get(key1)));
            System.out.println(byteBufferToString(dao.get(key2)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        // iteration
        try (DAO dao = DAOFactory.create(new File("/home/accakyra/temp/"))) {
            ByteBuffer key1 = stringToByteBuffer("key1");
            ByteBuffer key2 = stringToByteBuffer("key2");

            ByteBuffer value1 = stringToByteBuffer("value1");
            ByteBuffer value2 = stringToByteBuffer("value2");

            dao.upsert(key1, value1);
            dao.upsert(key2, value2);

            try (CloseableIterator<Record> iterator = dao.iterator()) {
                while (iterator.hasNext()) {
                    String value = byteBufferToString(iterator.next().getValue());
                    System.out.println(value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }        
        
        // open with custom config
        try (DAO dao = DAOFactory.create(new File("/home/accakyra/temp"),
                Config.builder().maxImmtableCount(1).memtableSize(32 * 1024 * 1024).build())) {
                    
        }
    }

    private static ByteBuffer stringToByteBuffer(String data) {
        return ByteBuffer.wrap(data.getBytes());
    }

    private static String byteBufferToString(ByteBuffer buffer) {
        int size = buffer.capacity();
        byte[] data = new byte[size];
        buffer.get(data);
        return new String(data);
    }
}
```
