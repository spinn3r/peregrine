package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.client.ScanRequest;
import peregrine.io.*;

/**
 *
 * Interface for describing an SSTable.  Both the memtable and disk tables have
 * to implement this interface.
 */
public interface SSTableReader extends SequenceReader {

    /**
     * <p> Seek to a given key (or keys) by using the SSTable index.  For disk
     * based indexes we use the meta block information.  For Memtable we can
     * just seek directly to the key in memory and read it from the internal
     * memory index.
     *
     * <p> The key() and value() method, when we match, must return the
     * <b>last</b> key we found via seekTo().
     */
    public boolean seekTo( List<StructReader> keys, RecordListener listener ) throws IOException;

    public void scan( ScanRequest scanRequest, RecordListener listener ) throws IOException;
    
}